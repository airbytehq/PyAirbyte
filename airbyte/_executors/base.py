# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
from __future__ import annotations

import os
import subprocess
from abc import ABC, abstractmethod
from contextlib import contextmanager
from threading import Event, Thread
from typing import IO, TYPE_CHECKING, Any, cast

from airbyte import exceptions as exc
from airbyte._message_iterators import AirbyteMessageIterator
from airbyte.http_caching.mitm_proxy import LOCALHOST


if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Iterator

    from airbyte.http_caching.cache import AirbyteConnectorCache
    from airbyte.sources.registry import ConnectorMetadata


_LATEST_VERSION = "latest"


class ExceptionHolder:
    def __init__(self) -> None:
        self.exception: Exception | None = None
        self.event = Event()

    def set_exception(
        self,
        ex: Exception,
    ) -> None:
        self.exception = ex
        self.event.set()  # Signal that an exception has occurred


def _pump_input(
    pipe: IO[str],
    messages: AirbyteMessageIterator,
    exception_holder: ExceptionHolder,
) -> None:
    """Pump lines into a pipe."""
    with pipe:
        try:
            pipe.writelines(message.model_dump_json() + "\n" for message in messages)
            pipe.flush()  # Ensure data is sent immediately
        except Exception as ex:
            exception_holder.set_exception(ex)


def _stream_from_file(file: IO[str]) -> Generator[str, Any, None]:
    """Stream lines from a file."""
    while True:
        line = file.readline()
        if not line:
            break
        yield line


@contextmanager
def _stream_from_subprocess(
    args: list[str],
    *,
    stdin: IO[str] | AirbyteMessageIterator | None = None,
    log_file: IO[str] | None = None,
    env: dict[str, str] | None = None,
) -> Generator[Iterable[str], None, None]:
    """Stream lines from a subprocess."""
    input_thread: Thread | None = None
    exception_holder = ExceptionHolder()
    if isinstance(stdin, AirbyteMessageIterator):
        process = subprocess.Popen(
            args,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=log_file,
            universal_newlines=True,
            encoding="utf-8",
            env=env,
        )
        input_thread = Thread(
            target=_pump_input,
            args=(
                process.stdin,
                stdin,
                exception_holder,
            ),
        )
        input_thread.start()
        input_thread.join()  # Ensure the input thread has finished

        # Don't bother raising broken pipe errors, as they only
        # indicate that a subprocess has terminated early.
        if exception_holder.exception and not isinstance(
            exception_holder.exception, BrokenPipeError
        ):
            raise exception_holder.exception

    else:
        # stdin is None or a file-like object
        process = subprocess.Popen(
            args,
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=log_file,
            universal_newlines=True,
            encoding="utf-8",
            env=env,
        )

    if process.stdout is None:
        raise exc.AirbyteSubprocessError(
            message="Subprocess did not return a stdout stream.",
            context={
                "args": args,
                "returncode": process.returncode,
            },
        )

    try:
        yield _stream_from_file(process.stdout)
        process.wait()
    finally:
        # Close the stdout stream
        if process.stdout:
            process.stdout.close()

        # Terminate the process if it is still running
        if process.poll() is None:  # Check if the process is still running
            process.terminate()
            try:
                # Wait for a short period to allow process to terminate gracefully
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # If the process does not terminate within the timeout, force kill it
                process.kill()

        # Now, the process is either terminated or killed. Check the exit code.
        exit_code = process.wait()

        # If the exit code is not 0 or -15 (SIGTERM), raise an exception
        if exit_code not in {0, -15}:
            raise exc.AirbyteSubprocessFailedError(
                run_args=args,
                exit_code=exit_code,
                original_exception=(
                    exception_holder.exception
                    if not isinstance(exception_holder.exception, BrokenPipeError)
                    else None
                ),
            )


class Executor(ABC):
    def __init__(
        self,
        *,
        name: str | None = None,
        metadata: ConnectorMetadata | None = None,
        target_version: str | None = None,
        http_cache: AirbyteConnectorCache | None = None,
    ) -> None:
        """Initialize a connector executor.

        The 'name' param is required if 'metadata' is None.
        """
        if not name and not metadata:
            raise exc.PyAirbyteInternalError(message="Either name or metadata must be provided.")

        self.name: str = (
            name or cast("ConnectorMetadata", metadata).name
        )  # metadata is not None here
        self.metadata: ConnectorMetadata | None = metadata
        self.enforce_version: bool = target_version is not None
        self.http_cache: AirbyteConnectorCache | None = http_cache

        self.reported_version: str | None = None
        self.target_version: str | None = None
        if target_version:
            if metadata and target_version == _LATEST_VERSION:
                self.target_version = metadata.latest_available_version
            else:
                self.target_version = target_version

    @property
    def _proxy_host(self) -> str | None:
        """Return the host name of the proxy server.

        This can be overridden in cases (like) docker, where we need to
        remap localhost to host.docker.internal.
        """
        if self.http_cache:
            return self.http_cache.proxy_host

        return None

    @property
    def _proxy_env_vars(self) -> dict[str, str]:
        """Return the environment variables for the proxy server.

        This is used to set the HTTP_PROXY and HTTPS_PROXY environment variables.
        """
        if not self.http_cache:
            return {}

        # Generally proxy_host will be 'host.docker.internal' or 'localhost'
        proxy_host = self._proxy_host
        proxy_port = self.http_cache.proxy_port

        if proxy_host and proxy_port:
            if proxy_host == LOCALHOST:
                proxy_host = "127.0.0.1"

            ca_cert_path = str(self.http_cache.ca_cert_path.absolute())
            return {
                "HTTP_PROXY": f"http://{proxy_host}:{proxy_port}",
                "HTTPS_PROXY": f"http://{proxy_host}:{proxy_port}",
                "NO_PROXY": f"localhost,127.0.0.1,{proxy_host}",
                "SSL_CERT_FILE": ca_cert_path,
                "REQUESTS_CA_BUNDLE": ca_cert_path,
            }

        return {}

    @property
    @abstractmethod
    def _cli(self) -> list[str]:
        """Get the base args of the CLI executable.

        Args will be appended to this list.
        """
        ...

    def map_cli_args(self, args: list[str]) -> list[str]:
        """Map CLI args if needed.

        By default, this is a no-op. Subclasses may override this method in order to
        map CLI args into the format expected by the connector.
        """
        return args

    def execute(
        self,
        args: list[str],
        *,
        stdin: IO[str] | AirbyteMessageIterator | None = None,
    ) -> Iterator[str]:
        """Execute a command and return an iterator of STDOUT lines.

        If stdin is provided, it will be passed to the subprocess as STDIN.
        """
        cli_cmd = [*self._cli, *self.map_cli_args(args)]
        env = self.env_vars
        print("Executing command:", " ".join(cli_cmd))
        with _stream_from_subprocess(
            cli_cmd,
            stdin=stdin,
            env=env,
        ) as stream_lines:
            yield from stream_lines

    @property
    def env_vars(self) -> dict[str, str]:
        """Get the environment variables for the connector.

        By default, this is a copy of `os.environ`. Subclasses may override this method
        to provide custom environment variables.

        In the future, we may reduce the number of environment variables we pass to the
        connector, but for now we pass all of them. This is useful for connectors that
        rely on environment variables to configure their behavior.
        """
        result = os.environ.copy()
        if self.http_cache:
            proxy_env_vars = self._proxy_env_vars
            print(f"Appending proxy environment variables: {proxy_env_vars}")
            result.update(proxy_env_vars)

        return result

    @abstractmethod
    def ensure_installation(self, *, auto_fix: bool = True) -> None:
        _ = auto_fix
        pass

    @abstractmethod
    def install(self) -> None:
        pass

    @abstractmethod
    def uninstall(self) -> None:
        pass

    def get_installed_version(
        self,
        *,
        raise_on_error: bool = False,
        recheck: bool = False,
    ) -> str | None:
        """Detect the version of the connector installed."""
        _ = raise_on_error, recheck  # Unused
        raise NotImplementedError(
            f"'{type(self).__name__}' class cannot yet detect connector versions."
        )
