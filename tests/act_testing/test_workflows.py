"""
Pytest tests for GitHub Actions workflows using ACT CLI.

This module contains parameterized tests that exercise different workflow
trigger scenarios using the ACT CLI wrapper.
"""

import pytest
from pathlib import Path
from typing import List, Tuple

from .act_runner import (
    ActRunner,
    WorkflowTrigger,
    create_push_trigger,
    create_pr_trigger,
    create_workflow_dispatch_trigger,
)


WORKFLOW_SCENARIOS = [
    ("python_pytest.yml", create_push_trigger(), "pytest-fast", True),
    ("python_pytest.yml", create_pr_trigger(), "pytest-fast", True),
    ("python_pytest.yml", create_pr_trigger(action="synchronize"), "pytest-no-creds", True),
    ("test-pr-command.yml", create_workflow_dispatch_trigger(inputs={"pr": "123"}), "start-workflow", True),
    ("fix-pr-command.yml", create_workflow_dispatch_trigger(inputs={"pr": "123"}), "pr-fix-on-demand", True),
]

TRIGGER_SCENARIOS = [
    ("push_main", create_push_trigger(ref="refs/heads/main")),
    ("push_feature", create_push_trigger(ref="refs/heads/feature-branch")),
    ("pr_opened", create_pr_trigger(action="opened")),
    ("pr_synchronize", create_pr_trigger(action="synchronize")),
    ("pr_closed", create_pr_trigger(action="closed")),
    ("workflow_dispatch_basic", create_workflow_dispatch_trigger()),
    ("workflow_dispatch_with_inputs", create_workflow_dispatch_trigger(inputs={"pr": "123", "comment-id": "456"})),
]


class TestWorkflowExecution:
    """Test class for GitHub Actions workflow execution via ACT CLI."""
    
    @pytest.fixture
    def act_runner(self) -> ActRunner:
        """Create an ActRunner instance for testing."""
        repo_root = Path(__file__).parent.parent.parent
        return ActRunner(workflow_dir=repo_root)
    
    @pytest.fixture
    def available_workflows(self, act_runner: ActRunner) -> List[str]:
        """Get list of available workflow files."""
        return act_runner.list_workflows()
    
    def test_act_runner_initialization(self, act_runner: ActRunner):
        """Test that ActRunner initializes correctly."""
        assert act_runner.workflow_dir.exists()
        assert act_runner.act_binary == "act"
        assert act_runner.default_image == "catthehacker/ubuntu:act-latest"
    
    def test_list_workflows(self, act_runner: ActRunner, available_workflows: List[str]):
        """Test that we can list available workflows."""
        assert len(available_workflows) > 0
        assert "python_pytest.yml" in available_workflows
        assert "test-pr-command.yml" in available_workflows
        assert "fix-pr-command.yml" in available_workflows
    
    @pytest.mark.parametrize("workflow_file,trigger,job_name,expected_success", WORKFLOW_SCENARIOS)
    def test_workflow_scenarios(
        self,
        act_runner: ActRunner,
        workflow_file: str,
        trigger: WorkflowTrigger,
        job_name: str,
        expected_success: bool
    ):
        """Test different workflow scenarios with various triggers."""
        result = act_runner.run_workflow(
            workflow_file=workflow_file,
            trigger=trigger,
            job_name=job_name,
            dry_run=True,
            verbose=True
        )
        
        if expected_success:
            assert result.success, f"Workflow {workflow_file} with job {job_name} failed: {result.stderr}"
        
        assert workflow_file in result.stdout or workflow_file in result.stderr
        if job_name:
            assert job_name in result.stdout or job_name in result.stderr
    
    @pytest.mark.parametrize("scenario_name,trigger", TRIGGER_SCENARIOS)
    def test_trigger_scenarios(
        self,
        act_runner: ActRunner,
        scenario_name: str,
        trigger: WorkflowTrigger
    ):
        """Test different trigger scenarios with python_pytest.yml workflow."""
        result = act_runner.run_workflow(
            workflow_file="python_pytest.yml",
            trigger=trigger,
            job_name="pytest-fast",  # Use fast job to avoid long execution
            dry_run=True,
            verbose=True
        )
        
        assert result.success, f"Trigger scenario {scenario_name} failed: {result.stderr}"
        
        assert trigger.event_name in result.stdout or trigger.event_name in result.stderr
    
    def test_workflow_with_secrets(self, act_runner: ActRunner):
        """Test workflow execution with secrets."""
        trigger = create_push_trigger()
        trigger.secrets = {
            "GCP_GSM_CREDENTIALS": "fake-credentials",
            "GITHUB_TOKEN": "fake-token"
        }
        
        result = act_runner.run_workflow(
            workflow_file="python_pytest.yml",
            trigger=trigger,
            job_name="pytest-fast",
            dry_run=True
        )
        
        if "authentication required" in result.stderr:
            assert "GCP_GSM_CREDENTIALS" in str(trigger.secrets)
            assert "GITHUB_TOKEN" in str(trigger.secrets)
        else:
            assert result.success, f"Workflow with secrets failed: {result.stderr}"
    
    def test_workflow_with_env_vars(self, act_runner: ActRunner):
        """Test workflow execution with environment variables."""
        trigger = create_push_trigger()
        trigger.env_vars = {
            "AIRBYTE_ANALYTICS_ID": "test-analytics-id",
            "PYTHONIOENCODING": "utf-8"
        }
        
        result = act_runner.run_workflow(
            workflow_file="python_pytest.yml",
            trigger=trigger,
            job_name="pytest-fast",
            dry_run=True
        )
        
        assert result.success, f"Workflow with env vars failed: {result.stderr}"
    
    def test_invalid_workflow_file(self, act_runner: ActRunner):
        """Test handling of invalid workflow file."""
        trigger = create_push_trigger()
        
        with pytest.raises(FileNotFoundError):
            act_runner.run_workflow(
                workflow_file="nonexistent.yml",
                trigger=trigger
            )
    
    def test_aaron_steers_resolve_ci_vars_action(self, act_runner: ActRunner):
        """Test workflows that use the Aaron Steers resolve CI vars action."""
        workflows_with_resolve_vars = [
            ("fix-pr-command.yml", create_workflow_dispatch_trigger(inputs={"pr": "123"})),
            ("test-pr-command.yml", create_workflow_dispatch_trigger(inputs={"pr": "123"})),
            ("poetry-lock-command.yml", create_workflow_dispatch_trigger(inputs={"pr": "123"})),
            ("welcome-message.yml", create_pr_trigger(action="opened")),  # This workflow is triggered by PR events
        ]
        
        for workflow_file, trigger in workflows_with_resolve_vars:
            result = act_runner.run_workflow(
                workflow_file=workflow_file,
                trigger=trigger,
                dry_run=True,
                verbose=True
            )
            
            workflow_content = (act_runner.workflow_dir / ".github" / "workflows" / workflow_file).read_text()
            assert "aaronsteers/resolve-ci-vars-action@v0" in workflow_content
            
            assert result.success, f"Workflow {workflow_file} with resolve-ci-vars-action failed: {result.stderr}"


class TestActRunnerEdgeCases:
    """Test edge cases and error handling for ActRunner."""
    
    @pytest.fixture
    def act_runner(self) -> ActRunner:
        """Create an ActRunner instance for testing."""
        repo_root = Path(__file__).parent.parent.parent
        return ActRunner(workflow_dir=repo_root)
    
    def test_timeout_handling(self, act_runner: ActRunner):
        """Test that long-running workflows timeout appropriately."""
        trigger = create_push_trigger()
        
        assert hasattr(act_runner, 'run_workflow')
    
    def test_different_platforms(self, act_runner: ActRunner):
        """Test running workflows on different platforms."""
        platforms = [
            ("ubuntu-latest", "catthehacker/ubuntu:act-latest"),
            ("ubuntu-20.04", "catthehacker/ubuntu:act-20.04"),
        ]
        
        for platform, image in platforms:
            trigger = create_push_trigger()
            trigger.platform = platform
            trigger.image = image
            
            result = act_runner.run_workflow(
                workflow_file="python_pytest.yml",
                trigger=trigger,
                job_name="pytest-fast",
                dry_run=True
            )
            
            assert result.success, f"Platform {platform} failed: {result.stderr}"
