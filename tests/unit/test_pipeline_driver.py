#!/usr/bin/env python3
"""
tests/unit/test_pipeline_driver.py

Unit tests for PipelineDriver class
"""

import pytest
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

from slurm_pipeline.core import PipelineDriver, ConfigParser


class MockApplication:
    """Mock application class for testing"""

    def __init__(self, app_name: str, jobs: list):
        self.app_name = app_name
        self.mock_jobs = jobs

    def validate_requirements(self):
        """Mock validation - can be overridden to raise exceptions"""
        pass

    def generate_jobs(self):
        """Return mock job list"""
        return self.mock_jobs


class TestPipelineDriver:
    """Test PipelineDriver functionality"""

    def test_initialization(self, temp_dir, sample_def_file):
        """Test PipelineDriver initialization"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        assert driver.working_dir == temp_dir
        assert len(driver.applications) == 0
        assert len(driver.job_scripts) == 0

    def test_add_application(self, temp_dir, sample_def_file):
        """Test adding applications to pipeline"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        mock_jobs = [
            {
                "job_name": "test_job_1",
                "type": "single",
                "script_path": str(temp_dir / "test1.sh"),
                "phase": "test",
            }
        ]

        mock_app = MockApplication("test_app", mock_jobs)
        driver.add_application("test_app", mock_app)

        assert "test_app" in driver.applications
        assert driver.applications["test_app"] == mock_app

    def test_generate_all_scripts(self, temp_dir, sample_def_file):
        """Test script generation for all applications"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create mock script files
        script1 = temp_dir / "dryrun.sh"
        script2 = temp_dir / "fillcf.sh"
        for script in [script1, script2]:
            script.write_text('#!/bin/bash\necho "mock script"')
            script.chmod(0o755)

        # Mock coyote jobs
        coyote_jobs = [
            {
                "job_name": "test_run_coyote_dryrun",
                "type": "single",
                "phase": "dryrun",
                "script_path": str(script1),
                "depends_on": None,
            },
            {
                "job_name": "test_run_coyote_fillcf",
                "type": "array",
                "phase": "fillcf",
                "script_path": str(script2),
                "depends_on_job": "test_run_coyote_dryrun",
            },
        ]

        mock_coyote = MockApplication("coyote", coyote_jobs)
        driver.add_application("coyote", mock_coyote)

        # Generate scripts
        generated_jobs = driver.generate_all_scripts(dry_run=True)

        assert "coyote" in generated_jobs
        assert len(generated_jobs["coyote"]) == 2
        assert len(driver.job_scripts) == 2

        # Check dependency tracking
        assert "test_run_coyote_fillcf" in driver.job_dependencies
        assert (
            "test_run_coyote_dryrun"
            in driver.job_dependencies["test_run_coyote_fillcf"]
        )

    def test_pipeline_validation_success(self, temp_dir, sample_def_file):
        """Test successful pipeline validation"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Add valid mock application
        mock_jobs = [
            {"job_name": "valid_job", "type": "single", "script_path": "/tmp/test.sh"}
        ]
        mock_app = MockApplication("valid_app", mock_jobs)
        driver.add_application("valid_app", mock_app)

        # Generate jobs to populate job_scripts
        driver.generate_all_scripts(dry_run=True)

        # Validate
        is_valid, errors = driver.validate_pipeline()

        assert is_valid is True
        assert len(errors) == 0

    def test_pipeline_validation_failure(self, temp_dir, sample_def_file):
        """Test pipeline validation with invalid application"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create mock application that fails validation
        mock_jobs = [
            {"job_name": "invalid_job", "type": "single", "script_path": "/tmp/test.sh"}
        ]
        mock_app = MockApplication("invalid_app", mock_jobs)

        # Override validation to raise exception
        mock_app.validate_requirements = MagicMock(
            side_effect=ValueError("Invalid config")
        )

        driver.add_application("invalid_app", mock_app)
        driver.generate_all_scripts(dry_run=True)

        # Validate
        is_valid, errors = driver.validate_pipeline()

        assert is_valid is False
        assert len(errors) == 1
        assert "Invalid config" in errors[0]

    def test_submission_order(self, temp_dir, sample_def_file):
        """Test job submission order calculation"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create jobs with dependencies: job1 -> job2 -> job3
        mock_jobs = [
            {"job_name": "job1", "type": "single", "script_path": "/tmp/job1.sh"},
            {
                "job_name": "job2",
                "type": "single",
                "script_path": "/tmp/job2.sh",
                "depends_on_job": "job1",
            },
            {
                "job_name": "job3",
                "type": "single",
                "script_path": "/tmp/job3.sh",
                "depends_on_job": "job2",
            },
        ]

        mock_app = MockApplication("test_app", mock_jobs)
        driver.add_application("test_app", mock_app)
        driver.generate_all_scripts(dry_run=True)

        submission_order = driver._get_submission_order()

        # job1 should come first, job3 should come last
        assert submission_order.index("job1") < submission_order.index("job2")
        assert submission_order.index("job2") < submission_order.index("job3")

    def test_circular_dependency_detection(self, temp_dir, sample_def_file):
        """Test detection of circular dependencies"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create jobs with circular dependency: job1 -> job2 -> job1
        mock_jobs = [
            {
                "job_name": "job1",
                "type": "single",
                "script_path": "/tmp/job1.sh",
                "depends_on_job": "job2",
            },
            {
                "job_name": "job2",
                "type": "single",
                "script_path": "/tmp/job2.sh",
                "depends_on_job": "job1",
            },
        ]

        mock_app = MockApplication("test_app", mock_jobs)
        driver.add_application("test_app", mock_app)
        driver.generate_all_scripts(dry_run=True)

        # Force circular dependency
        driver.job_dependencies["job1"] = ["job2"]
        driver.job_dependencies["job2"] = ["job1"]

        is_valid, errors = driver.validate_pipeline()

        assert is_valid is False
        assert any("Circular dependency" in error for error in errors)

    @patch("subprocess.run")
    def test_submit_pipeline_dry_run(self, mock_run, temp_dir, sample_def_file):
        """Test pipeline submission in dry run mode"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create mock script file
        script_path = temp_dir / "test_job.sh"
        script_path.write_text('#!/bin/bash\necho "test"')
        script_path.chmod(0o755)

        mock_jobs = [
            {
                "job_name": "test_job",
                "type": "single",
                "script_path": str(script_path),
                "phase": "test",
            }
        ]

        mock_app = MockApplication("test_app", mock_jobs)
        driver.add_application("test_app", mock_app)
        driver.generate_all_scripts(dry_run=True)

        # Mock successful sbatch call
        mock_run.return_value.stdout = "sbatch: Job submission test successful"
        mock_run.return_value.returncode = 0

        # Submit in dry run mode
        submitted_jobs = driver.submit_pipeline(dry_run=True)

        assert "test_job" in submitted_jobs
        assert submitted_jobs["test_job"].startswith("DRY_RUN_")

        # Verify sbatch was called with --test-only
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]  # First positional argument (command list)
        assert "sbatch" in call_args
        assert "--test-only" in call_args

    @patch("subprocess.run")
    def test_submit_pipeline_with_dependencies(
        self, mock_run, temp_dir, sample_def_file
    ):
        """Test pipeline submission with job dependencies"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create mock script files
        for script_name in ["job1.sh", "job2.sh"]:
            script_path = temp_dir / script_name
            script_path.write_text('#!/bin/bash\necho "test"')
            script_path.chmod(0o755)

        mock_jobs = [
            {
                "job_name": "job1",
                "type": "single",
                "script_path": str(temp_dir / "job1.sh"),
                "phase": "first",
            },
            {
                "job_name": "job2",
                "type": "single",
                "script_path": str(temp_dir / "job2.sh"),
                "phase": "second",
                "depends_on_job": "job1",
            },
        ]

        mock_app = MockApplication("test_app", mock_jobs)
        driver.add_application("test_app", mock_app)
        driver.generate_all_scripts(dry_run=True)

        # Mock sbatch responses
        mock_run.side_effect = [
            # First job submission
            MagicMock(stdout="Submitted batch job 12345", returncode=0),
            # Second job submission
            MagicMock(stdout="Submitted batch job 12346", returncode=0),
        ]

        # Submit pipeline
        submitted_jobs = driver.submit_pipeline(dry_run=False)

        assert len(submitted_jobs) == 2
        assert submitted_jobs["job1"] == "12345"
        assert submitted_jobs["job2"] == "12346"

        # Verify second job was submitted with dependency
        second_call_args = mock_run.call_args_list[1][0][0]
        assert "--dependency" in second_call_args
        dependency_idx = second_call_args.index("--dependency")
        assert "afterok:12345" in second_call_args[dependency_idx + 1]

    @patch("subprocess.run")
    def test_get_job_status(self, mock_run, temp_dir, sample_def_file):
        """Test job status retrieval"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Set up submitted jobs
        driver.submitted_jobs = {"job1": "12345", "job2": "DRY_RUN_job2"}

        # Mock squeue responses
        mock_run.side_effect = [
            MagicMock(stdout="RUNNING", returncode=0),  # job1 status
            # No call for DRY_RUN job
        ]

        status = driver.get_job_status()

        assert status["job1"] == "RUNNING"
        assert status["job2"] == "DRY_RUN"

    def test_print_pipeline_summary(self, temp_dir, sample_def_file, capsys):
        """Test pipeline summary printing"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        mock_jobs = [
            {
                "job_name": "test_job",
                "type": "single",
                "script_path": "/tmp/test.sh",
                "phase": "test",
            }
        ]

        mock_app = MockApplication("test_app", mock_jobs)
        driver.add_application("test_app", mock_app)
        driver.generate_all_scripts(dry_run=True)

        driver.print_pipeline_summary()

        captured = capsys.readouterr()
        assert "Pipeline Summary" in captured.out
        assert "test_app" in captured.out
        assert "test_job" in captured.out


class TestPipelineDriverIntegration:
    """Integration tests for PipelineDriver"""

    def test_full_pipeline_workflow(self, temp_dir, sample_def_file):
        """Test complete pipeline workflow without actual submission"""
        config = ConfigParser(str(sample_def_file))
        driver = PipelineDriver(config, str(temp_dir))

        # Create realistic mock jobs similar to CoyoteJob
        dryrun_script = temp_dir / "coyote_dryrun.sh"
        fillcf_script = temp_dir / "coyote_fillcf.sh"

        for script in [dryrun_script, fillcf_script]:
            script.write_text('#!/bin/bash\necho "mock coyote script"')
            script.chmod(0o755)

        coyote_jobs = [
            {
                "job_name": "test_run_coyote_dryrun",
                "type": "single",
                "phase": "dryrun",
                "script_path": str(dryrun_script),
                "depends_on": None,
            },
            {
                "job_name": "test_run_coyote_fillcf",
                "type": "array",
                "phase": "fillcf",
                "script_path": str(fillcf_script),
                "depends_on_job": "test_run_coyote_dryrun",
                "array_range": "0-7",
            },
        ]

        mock_coyote = MockApplication("coyote", coyote_jobs)
        driver.add_application("coyote", mock_coyote)

        # Full workflow
        generated_jobs = driver.generate_all_scripts(dry_run=True)
        is_valid, errors = driver.validate_pipeline()
        submission_order = driver._get_submission_order()

        # Assertions
        assert len(generated_jobs) == 1  # One application
        assert len(generated_jobs["coyote"]) == 2  # Two jobs
        assert is_valid is True
        assert len(errors) == 0
        assert submission_order[0] == "test_run_coyote_dryrun"  # Dryrun first
        assert submission_order[1] == "test_run_coyote_fillcf"  # Fillcf second

        print("Full pipeline workflow test passed! ✓")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
