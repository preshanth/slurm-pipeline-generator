# tests/unit/test_composition_classes.py
"""
Unit tests for composition classes
"""

import pytest
import tempfile
from pathlib import Path

from slurm_pipeline.core.components import (
    CommandBuilder,
    ResourceConfig,
    FileManager,
    ScriptGenerator,
)


class TestCommandBuilder:
    """Test CommandBuilder functionality"""

    def test_executable_setting(self):
        """Test setting executable"""
        cmd_builder = CommandBuilder()
        cmd_builder.set_executable("/path/to/coyote")
        assert cmd_builder.executable == "/path/to/coyote"

    def test_base_args(self):
        """Test base arguments"""
        cmd_builder = CommandBuilder()
        cmd_builder.set_executable("/path/to/coyote")
        cmd_builder.add_base_args(help="noprompt", vis="test.ms")

        command = cmd_builder.build_command()
        assert "/path/to/coyote" in command
        assert "help=noprompt" in command
        assert "vis=test.ms" in command

    def test_mode_args(self):
        """Test mode-specific arguments"""
        cmd_builder = CommandBuilder()
        cmd_builder.set_executable("/path/to/coyote")
        cmd_builder.add_mode_args("dryrun", cfcache="/path/to/cache")

        command = cmd_builder.build_command("dryrun")
        # If you want to check for mode, you should add it as a base or mode arg
        assert "cfcache=/path/to/cache" in command

    def test_python_command(self):
        """Test Python command building"""
        cmd_builder = CommandBuilder()

        command = cmd_builder.build_python_command(
            "script.py", arg1="value1", arg2="value2"
        )
        expected = ["python3", "script.py", "--arg1", "value1", "--arg2", "value2"]
        assert command == expected


class TestResourceConfig:
    """Test ResourceConfig functionality"""

    def test_memory_retrieval(self):
        """Test memory requirement retrieval"""
        slurm_config = {"coyote_mem": "4GB", "default_walltime": "2:00:00"}
        resource_config = ResourceConfig(slurm_config)

        assert resource_config.get_memory("coyote_mem") == "4GB"
        assert resource_config.get_memory("nonexistent", "8GB") == "8GB"

    def test_gpu_resources(self):
        """Test GPU resource mapping"""
        slurm_config = {"gpu_type": "h200"}
        resource_config = ResourceConfig(slurm_config)

        gpu_resources = resource_config.get_gpu_resources()
        assert gpu_resources["constraint"] == "h200"
        assert gpu_resources["gpu_mem"] == "141GB"

    def test_slurm_directives(self):
        """Test SLURM directive building"""
        slurm_config = {"account": "test_account", "email": "test@msu.edu"}
        resource_config = ResourceConfig(slurm_config)

        directives = resource_config.build_slurm_directives(job_name="test_job")
        assert directives["account"] == "test_account"
        assert directives["job_name"] == "test_job"


class TestFileManager:
    """Test FileManager functionality"""

    def test_initialization(self, temp_dir):
        """Test FileManager initialization"""
        file_manager = FileManager(str(temp_dir), "test_run")

        assert file_manager.working_dir == temp_dir
        assert file_manager.basename == "test_run"
        assert file_manager.logs_dir.exists()

    def test_iteration_filename(self, temp_dir):
        """Test iteration filename generation"""
        file_manager = FileManager(str(temp_dir), "test_run")

        assert file_manager.get_iteration_filename("psf", 1) == "test_run_iter001.psf"
        assert file_manager.get_iteration_filename("base") == "test_run.base"

    def test_log_paths(self, temp_dir):
        """Test log path generation"""
        file_manager = FileManager(str(temp_dir), "test_run")

        output_log, error_log = file_manager.get_log_paths("test_job")
        assert "test_job_%j.out" in output_log
        assert "test_job_%j.err" in error_log

        output_log, error_log = file_manager.get_log_paths("test_job", array_job=True)
        assert "test_job_%A_%a.out" in output_log
        assert "test_job_%A_%a.err" in error_log


class TestScriptGenerator:
    """Test ScriptGenerator functionality"""

    def test_directive_generation(self, temp_dir):
        """Test SLURM directive generation"""
        slurm_config = {"account": "test_account", "email": "test@msu.edu"}
        resource_config = ResourceConfig(slurm_config)
        file_manager = FileManager(str(temp_dir), "test_run")
        script_gen = ScriptGenerator(resource_config, file_manager)

        job_config = {
            "job_name": "test_job",
            "time": "1:00:00",
            "mem": "4GB",
            "account": "test_account",
        }

        directives = script_gen.generate_slurm_directives(job_config)
        assert "#SBATCH --job-name=test_job" in directives
        assert "#SBATCH --time=1:00:00" in directives
        assert "#SBATCH --mem=4GB" in directives

    def test_script_generation(self, temp_dir):
        """Test complete script generation"""
        slurm_config = {"account": "test_account", "email": "test@msu.edu"}
        resource_config = ResourceConfig(slurm_config)
        file_manager = FileManager(str(temp_dir), "test_run")
        script_gen = ScriptGenerator(resource_config, file_manager)

        job_config = {
            "job_name": "test_job",
            "time": "1:00:00",
            "mem": "4GB",
            "account": "test_account",
        }

        command = ["/path/to/coyote", "help=noprompt", "mode=dryrun"]
        script = script_gen.generate_script(job_config, command)

        assert "#!/bin/bash" in script
        assert "#SBATCH --job-name=test_job" in script
        assert "/path/to/coyote help=noprompt mode=dryrun" in script
