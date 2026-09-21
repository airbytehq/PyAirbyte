# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
"""Hosted Datadog contracts, with instrumentation isolated in fresh interpreters.

The recording writer is installed AFTER install() recreates ddtrace's writer. It
receives finished traces after the real export processors, including embedded
LLM Observability events. No Datadog agent URL is used for isolation. Socket
connections are blocked before instrumentation starts; all non-trace exporters
are disabled and the trace writer is replaced before any instrumented I/O.
Private ddtrace reads below are test-only contracts for the supported 4.x SDK.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import textwrap
from typing import Any

import pytest


_BOOTSTRAP = """
import asyncio
import copy
import json
import socket
from types import SimpleNamespace
from unittest.mock import patch, Mock

# Block Python socket connections before ddtrace startup. The native trace
# writer is replaced before instrumented I/O; all other exporters are disabled.
# HTTP tests use ASGITransport and requests uses a fake adapter.
def forbid_network(*args, **kwargs):
    raise AssertionError("unexpected test network connection")
socket.socket.connect = forbid_network
socket.socket.connect_ex = forbid_network
socket.create_connection = forbid_network

import ddtrace.auto
import ddtrace
from ddtrace.trace import tracer
from ddtrace.llmobs import LLMObs
from fastmcp import FastMCP, Client
from airbyte.mcp import _datadog as observability

class RecordingWriter:
    def __init__(self):
        self.traces = []
    def write(self, spans=None):
        if spans:
            # The writer boundary sees the complete sanitized APM export. Private
            # Span getters are necessary to include all meta structs, not a safe
            # subset of fields which could accidentally hide a leak from this test.
            self.traces.append(copy.deepcopy([{
                "name": s.name, "resource": s.resource, "service": s.service,
                "trace_id": s.trace_id, "span_id": s.span_id,
                "parent_id": s.parent_id, "error": s.error,
                "meta": s.get_tags(), "metrics": s.get_metrics(),
                "struct": s._get_meta_structs(),
                "events": [e.to_dict() for e in s._get_events()],
                "links": [link.to_dict() for link in s._get_links()],
            } for s in spans]))
    def recreate(self, **kwargs):
        return self
    def stop(self, *args, **kwargs):
        pass
    def flush_queue(self, *args, **kwargs):
        pass

writer = RecordingWriter()
def record_exports():
    # ddtrace 4.x exposes no public writer setter. Replace the aggregator writer
    # after install(), without configure() (which would replace the sanitizer).
    tracer._span_aggregator.writer = writer
record_exports()
"""


def _clean_env() -> dict[str, str]:
    """Drop inherited Datadog, Airbyte, MCP and OTel settings for child interpreters."""
    return {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("DD_", "AIRBYTE_", "MCP_", "OTEL_"))
    }


def _run(script: str, *, env: dict[str, str] | None = None) -> Any:  # noqa: ANN401
    """Run a scenario with fresh instrumentation and no inherited service secrets."""
    child_env = _clean_env()
    child_env.update({
        "DD_API_KEY": "test-only-not-a-real-api-key",
        "DD_SERVICE": "mcp-unit-test",
        "DD_ENV": "test",
        "DD_AGENTLESS_ENABLED": "1",
        "DD_LLMOBS_ENABLED": "1",
        "DD_MCP_CAPTURE_INTENT": "1",
        "DD_TRACE_ENABLED": "true",
        "DD_REMOTE_CONFIGURATION_ENABLED": "false",
        "DD_CRASHTRACKING_ENABLED": "false",
        "DD_INSTRUMENTATION_TELEMETRY_ENABLED": "false",
        "DD_TRACE_HTTPX_ENABLED": "false",
        "DD_TRACE_URLLIB3_ENABLED": "false",
        "DD_MCP_DISTRIBUTED_TRACING": "false",
        "DD_TRACE_PROPAGATION_STYLE_EXTRACT": "none",
        "DD_TRACE_SPAN_ATTRIBUTE_SCHEMA": "v0",
        "DD_TRACE_REMOVE_INTEGRATION_SERVICE_NAMES_ENABLED": "true",
        "DD_TRACE_HTTP_CLIENT_TAG_QUERY_STRING": "false",
        "DD_HTTP_SERVER_TAG_QUERY_STRING": "false",
        "DO_NOT_TRACK": "1",
        "AIRBYTE_MCP_INSIDERS": "1",
        "AIRBYTE_AGENTS_API_URL": "https://agents.example.com/api/v1",
    })
    child_env.update(env or {})
    result = subprocess.run(
        [sys.executable, "-c", _BOOTSTRAP + textwrap.dedent(script)],
        env=child_env,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    lines = [
        line.removeprefix("RESULT:")
        for line in result.stdout.splitlines()
        if line.startswith("RESULT:")
    ]
    return json.loads(lines[-1]) if lines else None


_CAPTURE = """
import httpx
import requests
from jsonschema import ValidationError
from fastmcp_extensions import CapabilityTokenMiddleware, mcp_tool
from airbyte.constants import set_hosted_mcp_mode
from airbyte.mcp import server, _tool_utils
from airbyte.mcp._transport_security import HostOriginGuardMiddleware
from airbyte.mcp.interactive import _registry_ui, _sync_history_ui, _workspace_sync_status_ui

app = server.app
from fastmcp.server.auth.providers.jwt import JWTVerifier, RSAKeyPair
from fastmcp.server.dependencies import get_access_token
keys = RSAKeyPair.generate()
bearer_token = keys.create_token(subject='jwt-subject-SENTINEL')
app.auth = JWTVerifier(public_key=keys.public_key)
set_hosted_mcp_mode()
# The Agents API gate must be open before listings, including on custom test roots.
_tool_utils.is_agents_api_available = lambda ctx: True

class Adapter(requests.adapters.BaseAdapter):
    def send(self, request, **kwargs):
        assert request.headers.get('x-datadog-trace-id')
        assert request.headers.get('traceparent')
        if request.url.endswith('/v1/track'):
            raise requests.ConnectionError('request-error-SENTINEL')
        response = requests.Response()
        response.status_code = 200
        response._content = b'{}'
        response.url = request.url
        return response
    def close(self):
        pass

http = requests.Session()
http.mount('https://', Adapter())
@app.tool()
@mcp_tool(read_only=True)
def secret_probe(secret: str, fail: bool = False) -> str:
    assert get_access_token().client_id == 'jwt-subject-SENTINEL'
    if fail:
        try:
            raise ValidationError('validation-SENTINEL: ' + secret)
        except ValidationError:
            http.post('https://api.segment.io/v1/track', json={'secret': secret})
    http.get('https://api.airbyte.com/v1/connections?token=query-SENTINEL')
    return 'result-SENTINEL:' + secret

# Register the probe through the same server-owned records as production tools.
observability.install(app)
record_exports()
# The production registry supplies module/hint tags; use an existing tool name
# for tag tests below instead of manufacturing registration records for the probe.
connection = SimpleNamespace(
    name='Test connection', job_history_url='',
    source=SimpleNamespace(name='Source', connector_url=''),
    destination=SimpleNamespace(name='Destination', connector_url=''),
    get_previous_sync_logs=lambda **kwargs: [],
)
workspace = SimpleNamespace(
    workspace_id='12345678-1234-1234-1234-123456789abc', workspace_url='https://example.com/workspace',
    get_connection=lambda **kwargs: connection,
    list_connections=lambda **kwargs: [],
)
_registry_ui._list_public_registry_connectors = lambda **kwargs: []
_sync_history_ui._get_cloud_workspace = lambda *args: workspace
_workspace_sync_status_ui._get_cloud_workspace = lambda *args: workspace

raw_app = app.http_app(path='/mcp', transport='streamable-http', stateless_http=True, json_response=True)
http_app = HostOriginGuardMiddleware(
    CapabilityTokenMiddleware(observability.SessionIdHeaderDigest(raw_app)), ('testserver',),
)
results = {'bearer_token': bearer_token}
# Observe only booleans before the real sanitizer so the test proves the chained
# error and unconditionally captured headers were present before removal.
original_sanitize = observability.ApmExportSanitizer.process_trace
raw_evidence = {'chain': False, 'useragent': False, 'referrer': False,
                'endpoint_scan': False, 'security_test': False, 'connection_id': False,
                'host_header': False, 'unknown_path': False, 'unknown_method': False}
def observe_sanitizer(self, trace):
    for span in trace:
        stack = span.get_tag('error.stack') or ''
        if span.name == 'requests.request' and 'validation-SENTINEL' in stack and 'argument-SENTINEL' in stack:
            raw_evidence['chain'] = True
        raw_evidence['useragent'] |= bool(span.get_tag('http.useragent'))
        raw_evidence['referrer'] |= bool(span.get_tag('http.referrer_hostname'))
        raw_evidence['endpoint_scan'] |= span.get_tag('http.request.headers.x-datadog-endpoint-scan') == 'scan-header-SENTINEL'
        raw_evidence['security_test'] |= span.get_tag('http.request.headers.x-datadog-security-test') == 'security-header-SENTINEL'
        raw_evidence['connection_id'] |= 'connection-secret-SENTINEL' in (span.get_tag('http.url') or '')
        raw_evidence['host_header'] |= 'host-header-SENTINEL' in (span.get_tag('http.url') or '')
        raw_evidence['unknown_path'] |= 'private-path-SENTINEL' in span.resource
        raw_evidence['unknown_method'] |= span.get_tag('http.method') == 'METHOD-SENTINEL'
    return original_sanitize(self, trace)
observability.ApmExportSanitizer.process_trace = observe_sanitizer
async def scenario():
    async with raw_app.router.lifespan_context(raw_app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app), base_url='http://testserver') as client:
            async def rpc(method, params, headers=None):
                start = len(writer.traces)
                response = await client.post('/mcp?code=oauth-SENTINEL', json={
                    'jsonrpc':'2.0', 'id':1, 'method':method, 'params':params,
                }, headers={
                    'accept':'application/json, text/event-stream',
                    'user-agent':'useragent-SENTINEL',
                    'referer':'https://referrer-SENTINEL.example.com/private',
                    'x-datadog-endpoint-scan':'scan-header-SENTINEL',
                    'x-datadog-security-test':'security-header-SENTINEL',
                    'authorization':'Bearer ' + bearer_token,
                    **(headers or {}),
                })
                assert response.status_code == 200, response.text
                tracer.flush()
                return {'body':response.json(), 'traces':copy.deepcopy(writer.traces[start:]),
                        'headers':dict(response.headers), 'status':response.status_code}
            results['host_header'] = await rpc('tools/call', {
                'name':'secret_probe', 'arguments':{'secret':'argument-SENTINEL'},
            }, {'host':'testserver:host-header-SENTINEL'})
            results['unknown_tool'] = await rpc('tools/call', {
                'name':'unknown_tool_name_SENTINEL', 'arguments':{'secret':'argument-SENTINEL'},
            })
            for key, method, path in [
                ('unknown_path', 'GET', '/private-path-SENTINEL'),
                ('unknown_method', 'METHOD-SENTINEL', '/mcp'),
            ]:
                start = len(writer.traces)
                response = await client.request(method, path)
                tracer.flush()
                results[key] = {'status':response.status_code,
                                'traces':copy.deepcopy(writer.traces[start:])}
            results['initialize'] = await rpc('initialize', {
                'protocolVersion':'2025-11-25',
                'capabilities':{'extensions':{'io.modelcontextprotocol/ui':{}}},
                'clientInfo':{'name':'test-client','version':'1'},
            })
            token = results['initialize']['headers']['mcp-session-id']
            results['token'] = token
            names = ['list_cloud_workspaces', 'execute_agent_connector_ro']
            results['parameters_before'] = {name:copy.deepcopy((await app.get_tool(name)).parameters) for name in names}
            for key in ['listing1', 'listing2']:
                results[key] = await rpc('tools/list', {}, {'mcp-session-id': token})
            results['parameters_after'] = {name:(await app.get_tool(name)).parameters for name in names}
            for key, args in [('optional', {'secret':'argument-SENTINEL'}),
                              ('intent', {'secret':'argument-SENTINEL', 'telemetry':{'intent':'Inspect current sync status'}}),
                              ('error', {'secret':'argument-SENTINEL', 'fail':True})]:
                results[key] = await rpc('tools/call', {'name':'secret_probe','arguments':args})
            for key, session in [('token_call',token), ('long_session','s'*600),
                                 ('jwt_session','eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZW50aW5lbCJ9.signature-SENTINEL')]:
                results[key] = await rpc('tools/call', {'name':'show_connectors_list','arguments':{
                    'telemetry':{'intent':'Browse available connectors'}}}, {
                    'mcp-session-id':session, 'x-mcp-extensions':'io.modelcontextprotocol/ui extra.extension',
                    'x-airbyte-workspace-id':'12345678-1234-1234-1234-123456789ABC',
                    'x-airbyte-organization-id':'87654321-4321-4321-4321-ABCDEF123456',
                })
            results['no_intent'] = await rpc('tools/call', {'name':'show_connectors_list','arguments':{}}, {
                'mcp-session-id':token, 'x-airbyte-workspace-id':'invalid-uuid-SENTINEL',
                'x-airbyte-organization-id':'invalid-org-SENTINEL'})
            for name in ['show_connectors_list','show_connection_sync_history','show_workspace_sync_status']:
                args = {'connection_id':'12345678-1234-1234-1234-123456789abc'} if name == 'show_connection_sync_history' else {}
                results[name] = await rpc('tools/call', {'name':name,'arguments':args}, {'mcp-session-id':token})
            results['rejected'] = await rpc('tools/call', {
                'name':'show_connectors_list','arguments':{'search':'filter-argument-SENTINEL'}}, {})
            results['http_context'] = await rpc('tools/call', {'name':'secret_probe','arguments':{'secret':'argument-SENTINEL'}}, {
                'baggage':'user.id=baggage-SENTINEL,account.id=account-SENTINEL,session.id=session-SENTINEL',
                'x-datadog-trace-id':'123456789', 'x-datadog-parent-id':'987654321',
                'x-datadog-tags':'_dd.p.user_id=datadog-tag-SENTINEL',
                'traceparent':'00-000000000000000000000000075bcd15-000000003ade68b1-01',
            })
            # Exercise the actual Cloud tool, workspace, connection and generated
            # SDK. Only authentication setup and the network adapter are replaced.
            from airbyte.mcp import cloud
            from airbyte.cloud import CloudWorkspace
            real_workspace = CloudWorkspace(
                workspace_id='12345678-1234-1234-1234-123456789abc',
                bearer_token='cloud-bearer-SENTINEL',
            )
            with patch.object(cloud, '_get_cloud_workspace', return_value=real_workspace):
                with patch.object(requests.Session, 'get_adapter', return_value=Adapter()):
                    for key, connection_id in [
                        ('malformed_connection', 'connection-secret-SENTINEL'),
                        ('valid_connection', '12345678-1234-1234-1234-123456789abc'),
                    ]:
                        results[key] = await rpc('tools/call', {
                            'name':'describe_cloud_connection', 'arguments':{'connection_id':connection_id},
                        })
            results['mcp_context'] = await rpc('tools/call', {
                'name':'secret_probe','arguments':{'secret':'argument-SENTINEL'},
                '_meta':{'_dd_trace_context':{'x-datadog-trace-id':'123456789','x-datadog-parent-id':'987654321',
                                             'x-datadog-tags':'_dd.p.user_id=meta-SENTINEL'}},
            })
asyncio.run(scenario())
results['all_traces'] = writer.traces
results['raw_evidence'] = raw_evidence
print('RESULT:' + json.dumps(results))
"""


@pytest.fixture(scope="module")
def exported() -> dict[str, Any]:
    """Capture real HTTP, MCP, and requests exports once in an isolated process."""
    return _run(_CAPTURE)


def _spans(capture: dict[str, Any]) -> list[dict[str, Any]]:
    """Flatten the actual recording writer output, retaining every server span."""
    return [span for trace in capture["traces"] for span in trace]


def _event(capture: dict[str, Any]) -> dict[str, Any]:
    """Require exactly one exported LLM tool event for one HTTP tool call."""
    events = [
        span["struct"]["_llmobs"]
        for span in _spans(capture)
        if "_llmobs" in span["struct"]
    ]
    assert len(events) == 1, events
    return events[0]


def _tool_span(capture: dict[str, Any]) -> dict[str, Any]:
    """Find the real ddtrace MCP server tool span."""
    return next(span for span in _spans(capture) if span["name"] == "mcp.tools/call")


def test_schema_shim_two_listings_keep_required_and_leave_tool_parameters_untouched(
    exported: dict[str, Any],
) -> None:
    """Both Cloud and Agents wire schemas retain their original optionality."""
    assert exported["parameters_before"] == exported["parameters_after"]
    for name, original in exported["parameters_before"].items():
        assert "telemetry" not in original.get("properties", {})
        for listing in ("listing1", "listing2"):
            wire = next(
                tool["inputSchema"]
                for tool in exported[listing]["body"]["result"]["tools"]
                if tool["name"] == name
            )
            assert wire.get("required", []) == original.get("required", [])
            assert "telemetry" in wire["properties"]
            assert "telemetry" not in wire.get("required", [])
            assert "intent" not in wire.get("required", [])


def test_telemetry_intent_is_optional_and_call_succeeds(
    exported: dict[str, Any],
) -> None:
    """Calling the same instrumented tool works with or without optional intent."""
    for key in ("optional", "intent"):
        assert not exported[key]["body"]["result"]["isError"]
        assert "result-SENTINEL:argument-SENTINEL" in json.dumps(exported[key]["body"])


def test_processor_redacts_input_output_and_sets_intent(
    exported: dict[str, Any],
) -> None:
    """Arguments/results are absent from every export while intent is retained."""
    event = _event(exported["intent"])
    assert event["name"] == "secret_probe"
    assert event["parent_id"] == "undefined"
    assert event["meta"]["span"]["kind"] == "tool"
    assert event["meta"]["intent"] == "Inspect current sync status"
    assert event["meta"]["input"]["value"] == "[redacted by airbyte-mcp]"
    assert event["meta"]["output"]["value"] == "[redacted by airbyte-mcp]"
    assert "SENTINEL" not in json.dumps(exported["all_traces"])
    spans = _spans(exported["intent"])
    root = next(span for span in spans if span["name"] == "starlette.request")
    tool = _tool_span(exported["intent"])
    child = next(span for span in spans if span["name"] == "requests.request")
    assert tool["parent_id"] == root["span_id"]
    assert child["parent_id"] == tool["span_id"]
    assert child["meta"]["http.url"] == "https://api.airbyte.com/v1/connections"
    assert child["meta"]["http.status_code"] == "200"


def test_processor_drops_initialize_event(exported: dict[str, Any]) -> None:
    """Initialize keeps its APM span but exports no LLM event."""
    spans = _spans(exported["initialize"])
    assert any(span["name"] == "mcp.initialize" for span in spans)
    assert all("_llmobs" not in span["struct"] for span in spans)


def test_sanitizer_strips_error_text_and_useragent_on_all_spans(
    exported: dict[str, Any],
) -> None:
    """Chained validation secrets disappear even from the failing requests child."""
    spans = _spans(exported["error"])
    assert all(exported["raw_evidence"].values())
    assert {"starlette.request", "mcp.tools/call", "requests.request"} <= {
        s["name"] for s in spans
    }
    child = next(span for span in spans if span["name"] == "requests.request")
    assert child["error"] == 1
    assert "ConnectionError" in child["meta"]["error.type"]
    assert "SENTINEL" not in json.dumps(exported["all_traces"])
    for trace in exported["all_traces"]:
        for span in trace:
            assert {
                "error.message",
                "error.msg",
                "error.stack",
                "http.useragent",
                "http.referrer_hostname",
                "http.request.headers.x-datadog-endpoint-scan",
                "http.request.headers.x-datadog-security-test",
            }.isdisjoint(span["meta"])


def test_tool_span_error_tags_use_cause_class(exported: dict[str, Any]) -> None:
    """The SDK error classification and original cause class survive redaction."""
    span = _tool_span(exported["error"])
    assert exported["error"]["body"]["result"]["isError"]
    assert span["error"] == 1
    assert span["meta"]["error.type"] == "ToolError"
    assert "error.message" not in span["meta"]
    assert "error.stack" not in span["meta"]
    assert _event(exported["error"])["tags"]["airbyte_error_type"] == "ConnectionError"


def test_filter_rejected_call_has_no_pyairbyte_tags(exported: dict[str, Any]) -> None:
    """Tool filters precede strip/tag middleware and reject non-UI clients there."""
    assert exported["rejected"]["body"]["result"]["isError"]
    assert _tool_span(exported["rejected"])["meta"]["error.type"] == "ToolError"
    assert not any(
        tag.startswith(("airbyte_", "intent_present"))
        for tag in _event(exported["rejected"])["tags"]
    )
    assert "filter-argument-SENTINEL" not in json.dumps(exported["rejected"]["traces"])


def test_session_id_digest_replaces_header_and_call_succeeds(
    exported: dict[str, Any],
) -> None:
    """Long/JWT sessions digest and token-only clients can call all real UI tools."""
    sessions = {
        "token_call": exported["token"],
        "long_session": "s" * 600,
        "jwt_session": "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJzZW50aW5lbCJ9.signature-SENTINEL",
    }
    for key, raw in sessions.items():
        capture = exported[key]
        assert not capture["body"]["result"]["isError"]
        digest = hashlib.sha256(raw.encode()).hexdigest()
        event = _event(capture)
        assert event["session_id"] == digest
        assert event["tags"]["mcp_session_id"] == digest
        assert raw not in json.dumps(exported["all_traces"])
    listed = {tool["name"] for tool in exported["listing2"]["body"]["result"]["tools"]}
    for name in (
        "show_connectors_list",
        "show_connection_sync_history",
        "show_workspace_sync_status",
    ):
        assert name in listed
        assert not exported[name]["body"]["result"]["isError"]
        assert exported[name]["body"]["result"]["structuredContent"]


def test_tool_span_tags_session_and_uuid_tags(exported: dict[str, Any]) -> None:
    """Real registry hints, intent, lowercased UUIDs, and session reach event tags."""
    event = _event(exported["token_call"])
    assert {
        "airbyte_tool_module:interactive",
        "intent_present:true",
        "airbyte_workspace_id:12345678-1234-1234-1234-123456789abc",
        "airbyte_organization_id:87654321-4321-4321-4321-abcdef123456",
        "airbyte_tool_mutating:false",
        "airbyte_tool_destructive:false",
    } <= {f"{key}:{value}" for key, value in event["tags"].items()}
    assert event["session_id"] == hashlib.sha256(exported["token"].encode()).hexdigest()
    no_intent = _event(exported["no_intent"])
    assert no_intent["tags"]["intent_present"] == "false"
    assert not any(
        tag.startswith(("airbyte_workspace_id", "airbyte_organization_id"))
        for tag in no_intent["tags"]
    )
    assert "mcp_client_id" not in json.dumps(exported["all_traces"])
    assert "jwt-subject-SENTINEL" not in json.dumps(exported["all_traces"])
    assert exported["bearer_token"] not in json.dumps(exported["all_traces"])


def test_inbound_http_propagation_and_baggage_ignored(exported: dict[str, Any]) -> None:
    """Neither HTTP trace context nor baggage may join or annotate server traces."""
    spans = _spans(exported["http_context"])
    assert spans
    assert all(span["trace_id"] != 123456789 for span in spans)
    assert all(span["parent_id"] != 987654321 for span in spans)
    assert "baggage." not in json.dumps(spans)
    assert "SENTINEL" not in json.dumps(spans)


def test_inbound_mcp_meta_context_ignored(exported: dict[str, Any]) -> None:
    """MCP's independent context extraction path is disabled as well."""
    spans = _spans(exported["mcp_context"])
    assert spans
    assert all(span["trace_id"] != 123456789 for span in spans)
    assert all(span["parent_id"] != 987654321 for span in spans)
    assert "meta-SENTINEL" not in json.dumps(spans)


def test_strip_keeps_cached_clients_working_when_llmobs_disabled() -> None:
    """Cached telemetry remains accepted with no active Datadog instrumentation."""
    _run(
        """
        app = FastMCP('cached-client')
        @app.tool()
        def echo(value: str) -> str:
            return value
        observability.install(app)
        async def check():
            async with Client(app) as client:
                result = await client.call_tool('echo', {'value':'ok', 'telemetry':{'intent':'cached'}})
                assert result.data == 'ok'
        asyncio.run(check())
    """,
        env={"DD_LLMOBS_ENABLED": "0", "DD_TRACE_ENABLED": "false"},
    )


def test_strip_preserves_real_telemetry_parameter() -> None:
    """The rollback strip leaves a tool's genuine telemetry argument untouched."""
    _run(
        """
        app = FastMCP('real-parameter')
        @app.tool()
        def echo(telemetry: str) -> str:
            return telemetry
        observability.install(app, environ={})
        async def check():
            async with Client(app) as client:
                result = await client.call_tool('echo', {'telemetry':'native-value'})
                assert result.data == 'native-value'
        asyncio.run(check())
    """,
        env={"DD_LLMOBS_ENABLED": "0", "DD_TRACE_ENABLED": "false"},
    )


def test_processor_returns_none_on_internal_error() -> None:
    """A processor error drops the event rather than exporting original data."""
    _run("""
        broken = Mock()
        broken.get_tag.side_effect = RuntimeError('processor-SENTINEL')
        assert observability.redact_llmobs_span(broken) is None
        app = FastMCP('processor-failure')
        observability.install(app)
        record_exports()
        # Register a wrapper exercising the real processor's fail-closed result
        # through LLMObs's export pipeline, not merely calling a mock in isolation.
        LLMObs.register_processor(lambda event: observability.redact_llmobs_span(broken))
        with LLMObs.tool(name='failed-redaction'):
            LLMObs.annotate(input_data='processor-input-SENTINEL')
        tracer.flush()
        assert writer.traces
        assert all('_llmobs' not in span['struct'] for trace in writer.traces for span in trace)
        assert 'SENTINEL' not in json.dumps(writer.traces)
    """)


def test_processor_registered_on_live_llmobs_instance() -> None:
    """Check the live private processor slot, because ddtrace has no public getter."""
    _run("""
        observability.install(FastMCP('registration'))
        record_exports()
        # Private SDK read is confined to this test; runtime must never assert it.
        assert LLMObs._instance._user_span_processor is observability.redact_llmobs_span
    """)


def test_sanitizer_drops_segment_batch_root_keeps_track_child_and_other_roots() -> None:
    """Test root-only Segment exclusion at the actual writer boundary."""
    _run("""
        observability.install(FastMCP('segment'))
        record_exports()
        with tracer.trace('requests.request') as span:
            span.set_tag('http.url', 'https://api.segment.io/v1/batch')
        assert not writer.traces
        with tracer.trace('starlette.request'):
            with tracer.trace('requests.request') as span:
                span.set_tag('http.url', 'https://api.segment.io/v1/track')
        with tracer.trace('requests.request') as span:
            span.set_tag('http.url', 'https://api.airbyte.com/v1/connections')
        tracer.flush()
        assert len(writer.traces) == 2
        urls = [s['meta'].get('http.url') for t in writer.traces for s in t]
        assert 'https://api.segment.io/v1/track' in urls
        assert 'https://api.airbyte.com/v1/connections' in urls
    """)


def test_sanitizer_drops_trace_on_internal_error() -> None:
    """A failed tag removal causes the real pipeline to drop the whole trace."""
    _run("""
        observability.install(FastMCP('broken-sanitizer'))
        record_exports()
        from ddtrace.trace import Span
        with patch.object(Span, 'remove_tag', side_effect=RuntimeError('sanitizer-SENTINEL')):
            with tracer.trace('starlette.request') as span:
                span.set_tag('http.useragent', 'header-SENTINEL')
        tracer.flush()
        assert writer.traces == []
    """)


@pytest.mark.parametrize("control", ["processor", "sanitizer"])
def test_install_disables_export_when_mandatory_control_fails(control: str) -> None:
    """Failure of either mandatory registration disables both export channels."""
    _run(f"""
        app = FastMCP('registration-failure')
        target = LLMObs if {control!r} == 'processor' else tracer
        method = 'register_processor' if {control!r} == 'processor' else 'configure'
        with patch.object(target, method, side_effect=RuntimeError('registration failed')):
            with patch.object(observability.logger, 'error') as log:
                observability.install(app)
                assert log.called
        assert not LLMObs.enabled
        assert not tracer.enabled
        assert len(app.middleware) == 2  # FastMCP dereference + unconditional strip
        async def check():
            @app.tool()
            def echo(value: str) -> str:
                return value
            async with Client(app) as client:
                assert (await client.call_tool('echo', {{'value':'still serving'}})).data == 'still serving'
        asyncio.run(check())
        assert writer.traces == []
    """)


def test_install_is_idempotent_and_respects_explicit_environ() -> None:
    """An explicit empty mapping overrides the process env and fixes install state."""
    _run("""
        app = FastMCP('idempotent', instructions='Original')
        with patch.object(LLMObs, 'register_processor') as register:
            observability.install(app, environ={})
            before = list(app.middleware)
            observability.install(app, environ={'DD_LLMOBS_ENABLED':'1'})
            assert app.middleware == before
            register.assert_not_called()
        assert app.instructions == 'Original'
        assert sum(isinstance(m, observability.TelemetryArgumentStripMiddleware) for m in app.middleware) == 1
    """)


def _import_http_main_modules(env: dict[str, str]) -> set[str]:
    """Import the hosted entrypoint in a fresh interpreter and return sys.modules."""
    child_env = _clean_env()
    child_env.update({
        "DD_TRACE_ENABLED": "false",
        "DD_TRACE_AGENT_URL": "http://127.0.0.1:1",
        "DD_REMOTE_CONFIGURATION_ENABLED": "false",
        "DD_CRASHTRACKING_ENABLED": "false",
        "DD_INSTRUMENTATION_TELEMETRY_ENABLED": "false",
        "DO_NOT_TRACK": "1",
        **env,
    })
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import airbyte.mcp.http_main, sys; print(*sys.modules, sep='\\n')",
        ],
        env=child_env,
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return set(result.stdout.splitlines())


def test_http_main_import_skips_ddtrace_without_api_key() -> None:
    """Without DD_API_KEY the entrypoint imports nothing from ddtrace."""
    assert "ddtrace" not in _import_http_main_modules({})


def test_http_main_import_bootstraps_ddtrace_with_api_key() -> None:
    """With DD_API_KEY the entrypoint self-instruments through ddtrace.auto."""
    modules = _import_http_main_modules({"DD_API_KEY": "0" * 32})
    assert {"ddtrace", "ddtrace.auto"} <= modules


def test_install_never_raises_without_ddtrace() -> None:
    """A missing ddtrace keeps hosted cached clients working."""
    _run(
        """
        import sys
        app = FastMCP('missing-package')
        with patch.dict(sys.modules, {'ddtrace':None}):
            observability.install(app, environ={'DD_LLMOBS_ENABLED':'1'})
        assert any(isinstance(m, observability.TelemetryArgumentStripMiddleware) for m in app.middleware)
        assert not any(isinstance(m, observability.DatadogToolSpanMiddleware) for m in app.middleware)
    """,
        env={"DD_LLMOBS_ENABLED": "0", "DD_TRACE_ENABLED": "false"},
    )


def test_install_is_noop_when_llmobs_disabled_except_strip() -> None:
    """Disabled install changes only the middleware required for cached clients."""
    _run(
        """
        import builtins
        app = FastMCP('disabled', instructions='unchanged')
        original_import = builtins.__import__
        def checked_import(name, *args, **kwargs):
            assert not name.startswith('ddtrace'), name
            return original_import(name, *args, **kwargs)
        before = list(app.middleware)
        with patch.object(builtins, '__import__', checked_import):
            observability.install(app, environ={})
        assert app.middleware[:-1] == before
        assert isinstance(app.middleware[-1], observability.TelemetryArgumentStripMiddleware)
        assert app.instructions == 'unchanged'
    """,
        env={"DD_LLMOBS_ENABLED": "0", "DD_TRACE_ENABLED": "false"},
    )


def test_install_keeps_apm_disabled_when_trace_disabled() -> None:
    """configure() must not silently undo the operator's DD_TRACE_ENABLED=false."""
    _run(
        """
        assert LLMObs.enabled
        assert tracer.enabled is False
        observability.install(FastMCP('disabled-apm'))
        record_exports()
        assert tracer.enabled is False
    """,
        env={"DD_TRACE_ENABLED": "false"},
    )


@pytest.mark.parametrize("version", ["", "operator-version"])
def test_version_tag_set_only_when_unset(version: str) -> None:
    """An explicit DD_VERSION wins over the installed distribution version."""
    _run(
        f"""
        from airbyte.version import get_version
        observability.install(FastMCP('version'))
        record_exports()
        assert ddtrace.config.version == ({version!r} or get_version())
        with tracer.trace('version-check'):
            pass
        assert writer.traces[0][0]['meta']['version'] == ({version!r} or get_version())
    """,
        env={"DD_VERSION": version},
    )


@pytest.mark.parametrize("llmobs,capture", [("0", "1"), ("1", "0"), ("1", "1")])
def test_instructions_sentence_requires_capture_flag(llmobs: str, capture: str) -> None:
    """The guidance sentence requires both enablement and schema advertisement."""
    _run(
        f"""
        app = FastMCP('instructions', instructions='original')
        observability.install(app)
        record_exports()
        observability.install(app)
        sentence = observability.INTENT_INSTRUCTIONS_SENTENCE.strip()
        assert app.instructions.count(sentence) == {int(llmobs == capture == "1")}
        assert app.instructions.startswith('original')
    """,
        env={"DD_LLMOBS_ENABLED": llmobs, "DD_MCP_CAPTURE_INTENT": capture},
    )


def test_no_httpx_span_from_server_import() -> None:
    """Import-time OIDC discovery really runs, with HTTPX instrumentation disabled."""
    _run("""
        import os
        import httpx
        from key_value.aio.stores.memory import MemoryStore
        def memory_store(**kwargs):
            return MemoryStore()
        os.environ.update({
            'MCP_SERVER_URL':'https://mcp.example.com',
            'AIRBYTE_MCP_OIDC_CLIENT_ID':'test-client',
            'AIRBYTE_MCP_OIDC_CLIENT_SECRET':'client-secret-SENTINEL',
            'AIRBYTE_MCP_OIDC_CLIENT_STORAGE_FACTORY':'__main__:memory_store',
            'AIRBYTE_MCP_OIDC_CONFIG_URL':'https://identity.example.com/.well-known/openid-configuration',
        })
        calls = []
        def discovery(request):
            calls.append(str(request.url))
            return httpx.Response(200, json={
                'issuer':'https://identity.example.com',
                'authorization_endpoint':'https://identity.example.com/authorize',
                'token_endpoint':'https://identity.example.com/token',
                'jwks_uri':'https://identity.example.com/jwks',
                'response_types_supported':['code'],
                'subject_types_supported':['public'],
                'id_token_signing_alg_values_supported':['RS256'],
            })
        transport = httpx.MockTransport(discovery)
        with patch.object(httpx.Client, '_transport_for_url', return_value=transport):
            from airbyte.mcp import server
        assert server.app.auth is not None
        assert calls
        tracer.flush()
        assert not any(s['name'].startswith('httpx') for t in writer.traces for s in t)
        assert 'SENTINEL' not in json.dumps(writer.traces)
    """)


def test_wrap_http_app_places_session_digest_innermost() -> None:
    """Capture the actual entrypoint wrapper and inspect its ASGI nesting."""
    _run(
        """
        from airbyte.mcp import http_main
        captured = {}
        def run(app, **kwargs):
            captured.update(kwargs)
        with patch.object(http_main, 'run_mcp_http_server', run):
            http_main.main()
        record_exports()
        sentinel = object()
        wrapped = captured['wrapper'](sentinel)
        chain = []
        while wrapped is not sentinel:
            chain.append(type(wrapped).__name__)
            wrapped = getattr(wrapped, '_app', getattr(wrapped, 'app', None))
            assert wrapped is not None, chain
        assert chain[-1] == 'SessionIdHeaderDigest', chain
        assert chain.count('SessionIdHeaderDigest') == 1
        assert captured['stateless_http'] is True
    """,
        env={"DD_LLMOBS_ENABLED": "0", "DD_TRACE_ENABLED": "false"},
    )


def test_real_cloud_connection_argument_cannot_escape_through_url(
    exported: dict[str, Any],
) -> None:
    """The real Cloud SDK cannot export an arbitrary connection argument as a path."""
    bad = _spans(exported["malformed_connection"])
    good = _spans(exported["valid_connection"])
    assert exported["raw_evidence"]["connection_id"]
    bad_child = next(span for span in bad if span["name"] == "requests.request")
    good_child = next(span for span in good if span["name"] == "requests.request")
    assert bad_child["meta"]["http.url"] == "[redacted by airbyte-mcp]"
    assert bad_child["resource"] == "GET [redacted by airbyte-mcp]"
    assert "out.host" not in bad_child["meta"]
    path = "/v1/connections/12345678-1234-1234-1234-123456789abc"
    assert good_child["meta"]["http.url"] == "https://api.airbyte.com" + path
    assert good_child["resource"] == "GET " + path
    assert good_child["meta"]["http.status_code"] == "200"
    assert "SENTINEL" not in json.dumps(exported["all_traces"])


@pytest.mark.parametrize(
    ("case", "status", "method", "route"),
    [
        ("host_header", 200, "POST", "/mcp"),
        ("unknown_path", 404, "GET", None),
        ("unknown_method", 405, "HTTP", "/mcp"),
    ],
)
def test_inbound_http_targets_are_sanitized(
    exported: dict[str, Any], case: str, status: int, method: str, route: str | None
) -> None:
    """Accepted Host values and rejected targets cannot escape through inbound spans."""
    capture = exported[case]
    assert capture["status"] == status
    assert exported["raw_evidence"][case]
    root = next(span for span in _spans(capture) if span["name"] == "starlette.request")
    assert root["meta"]["http.status_code"] == str(status)
    assert root["meta"]["http.method"] == method
    assert root["meta"].get("http.route") == route
    assert root["resource"] == f"{method} {route or '[redacted by airbyte-mcp]'}"
    assert {
        "http.url",
        "http.query.string",
        "out.host",
        "server.address",
        "peer.hostname",
        "network.destination.name",
    }.isdisjoint(root["meta"])
    assert "SENTINEL" not in json.dumps(exported["all_traces"])


def test_unknown_tool_name_does_not_escape_in_export(exported: dict[str, Any]) -> None:
    """Unknown names cannot become free-text tags; registered names stay visible."""
    capture = exported["unknown_tool"]
    assert capture["body"]["result"]["isError"]
    assert "unknown_tool_name_SENTINEL" in json.dumps(capture["body"])
    span = _tool_span(capture)
    assert span["error"] == 1
    assert span["meta"]["error.type"] == "ToolError"
    assert all("_llmobs" not in item["struct"] for item in _spans(capture))
    assert _event(exported["intent"])["name"] == "secret_probe"
    assert _event(exported["no_intent"])["name"] == "show_connectors_list"
    assert "SENTINEL" not in json.dumps(exported["all_traces"])


_ROUTE_UUID = "12345678-1234-1234-1234-123456789abc"
_SAFE_ROUTES = (
    *(
        f"https://api.airbyte.com/v1/{resource}/{_ROUTE_UUID}"
        for resource in ("connections", "sources", "destinations", "workspaces")
    ),
    "https://api.airbyte.com/v1/jobs/12345",
    "https://api.airbyte.com/v1/applications/token",
    "https://cloud.airbyte.com/api/v1/sources/check_connection",
    f"https://api.airbyte.ai/api/v1/workspaces/{_ROUTE_UUID}",
    f"https://api.airbyte.ai/api/v1/integrations/connectors/{_ROUTE_UUID}/execute",
    f"https://api.airbyte.ai/api/v1/integrations/connectors/{_ROUTE_UUID}/inspect",
    "https://api.airbyte.ai/api/v1/skills/docs",
)
_UNSAFE_ROUTES = (
    *(
        f"https://api.airbyte.com/v1/{resource}/path-SENTINEL"
        for resource in ("connections", "sources", "destinations", "workspaces", "jobs")
    ),
    "https://api.airbyte.com/v1/jobs/12345678901234567890",
    "https://api.airbyte.com/v1/jobs/１２３４５",
    "https://api.airbyte.com/v1/connections/path%2FSENTINEL",
    "https://api.airbyte.com/v1/connections/path%252FSENTINEL",
    f"https://api.airbyte.com/v1/connections/{_ROUTE_UUID}/extra-SENTINEL",
    f"https://api.airbyte.ai/api/v1/integrations/connectors/{_ROUTE_UUID}/path-SENTINEL",
    "https://api.airbyte.ai/api/v1/workspaces/path-SENTINEL",
    "https://cloud.airbyte.com/api/v1/sources/path-SENTINEL",
    f"https://host-SENTINEL.example.com/v1/connections/{_ROUTE_UUID}",
    f"https://api.airbyte.com.host-SENTINEL.example.com/v1/connections/{_ROUTE_UUID}",
    f"https://api.airbyte.com:8443/v1/connections/{_ROUTE_UUID}",
    f"http://api.airbyte.com/v1/connections/{_ROUTE_UUID}",
    "https://connectors.airbyte.com/files/registries/registry-SENTINEL.json",
)
_ROUTE_CASES = (
    *((url, url) for url in _SAFE_ROUTES),
    *((url, None) for url in _UNSAFE_ROUTES),
    (
        f"https://api.airbyte.com/v1/connections/{_ROUTE_UUID}?token=query-SENTINEL#fragment-SENTINEL",
        f"https://api.airbyte.com/v1/connections/{_ROUTE_UUID}",
    ),
    (
        f"https://user-SENTINEL:password-SENTINEL@api.airbyte.com/v1/connections/{_ROUTE_UUID}",
        f"https://api.airbyte.com/v1/connections/{_ROUTE_UUID}",
    ),
)


@pytest.fixture(scope="module")
def route_exports() -> list[list[dict[str, Any]]]:
    """Run the route matrix through real requests spans in one isolated process."""
    return _run(f"""
        import requests
        observability.install(FastMCP('route-matrix'))
        record_exports()
        class Adapter(requests.adapters.BaseAdapter):
            def send(self, request, **kwargs):
                response = requests.Response()
                response.status_code = 204
                response._content = b''
                response.url = request.url
                return response
            def close(self):
                pass
        session = requests.Session()
        session.mount('https://', Adapter())
        session.mount('http://', Adapter())
        captures = []
        for url, expected in {_ROUTE_CASES!r}:
            start = len(writer.traces)
            with tracer.trace('starlette.request'):
                session.get(url)
            tracer.flush()
            captures.append([s for t in writer.traces[start:] for s in t])
        print('RESULT:' + json.dumps(captures))
    """)


@pytest.mark.parametrize("case", range(len(_ROUTE_CASES)))
def test_outbound_route_export_validates_origin_path_and_identifier(
    route_exports: list[list[dict[str, Any]]],
    case: int,
) -> None:
    """Only approved origins/routes with UUID or numeric identifiers survive."""
    _, expected = _ROUTE_CASES[case]
    spans = route_exports[case]
    child = next(span for span in spans if span["name"] == "requests.request")
    assert "sentinel" not in json.dumps(spans).lower()
    assert child["meta"]["http.status_code"] == "204"
    if expected is None:
        assert child["meta"]["http.url"] == "[redacted by airbyte-mcp]"
        assert child["resource"] == "GET [redacted by airbyte-mcp]"
        assert "out.host" not in child["meta"]
    else:
        assert child["meta"]["http.url"] == expected
        assert child["resource"] == "GET /" + expected.split("/", 3)[3]
        assert child["meta"]["out.host"] == expected.split("/")[2]
