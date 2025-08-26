"""
CoyoteJob - Concrete implementation for Coyote convolution function generation

Combines SingleJob (for dryrun) and ArrayJob (for fillcf) capabilities
Uses clean composition and inheritance from base job classes
"""

import os
import json
from typing import Dict, List, Any, Optional
from pathlib import Path

from ..core.single_job import SingleJob
from ..core.array_job import ArrayJob


class CoyoteJob(SingleJob, ArrayJob):
    """
    Coyote job implementation supporting both dryrun and fillcf modes

    - dryrun: Single job to create empty CF cache
    - fillcf: Array job to fill CFs in parallel
    """

    def __init__(
        self, config_parser: Any, working_dir: str = ".", coyote_binary: str = "coyote"
    ) -> None:
        """
        Initialize CoyoteJob

        Args:
            config_parser: Configuration parser instance
            working_dir: Working directory for scripts and logs
            coyote_binary: Path to coyote executable
        """
        # Set coyote-specific attributes first (needed by setup_command_builder)
        self.coyote_binary = str(Path(coyote_binary).resolve())
        # Initialize both parent classes (SingleJob calls BaseJob.__init__)
        SingleJob.__init__(self, config_parser, working_dir)
        self.nprocs = int(self.slurm_config.get("coyote_nprocs", 40))
        # Paths for generated files
        self.worker_module_path = self.file_manager.working_dir / "coyote_worker.py"
        self.common_params_file = self.file_manager.working_dir / "common_params.json"
        self.app_params_file = self.file_manager.working_dir / "app_params.json"
        # Validate coyote setup
        self.validate_coyote_requirements()

    def get_app_name(self) -> str:
        """Return application name for parameter lookup"""
        return "coyote"

    def setup_command_builder(self) -> None:
        """Setup coyote-specific command builder"""
        self.command_builder.set_executable(self.coyote_binary)
        self.command_builder.add_base_args(help="noprompt")
        # Add common parameters
        for key, value in self.common_params.items():
            if key not in ["basename", "iterations"]:  # Skip non-coyote params
                self.command_builder.add_base_args(**{key: value})
        # Add app-specific parameters
        for key, value in self.app_params.items():
            self.command_builder.add_base_args(**{key: value})
        # Add mode-specific arguments
        cfcache_path = self.get_cfcache_path()
        self.command_builder.add_mode_args("dryrun", cfcache=cfcache_path)
        self.command_builder.add_mode_args("fillcf", cfcache=cfcache_path)

    def validate_coyote_requirements(self) -> None:
        """Validate coyote-specific requirements"""
        # Call base validation
        self.validate_requirements()

        # Check coyote binary
        if not Path(self.coyote_binary).exists():
            raise FileNotFoundError(f"Coyote binary not found: {self.coyote_binary}")

        # Check required SLURM parameters
        if "coyote_nprocs" not in self.slurm_config:
            raise ValueError(
                "Missing required parameter: coyote_nprocs in [slurm] section"
            )

    def get_cfcache_path(self) -> str:
        """Get absolute path to CF cache directory"""
        cfcache_name = self.app_params.get("cfcache", "ps.cf")

        if Path(cfcache_name).is_absolute():
            return str(cfcache_name)

        return str(self.file_manager.working_dir / cfcache_name)

    def create_worker_module(self) -> str:
        """Create the coyote_worker.py module in working directory"""
        # Find the reference worker module
        worker_source = self._find_worker_module()

        # Copy to working directory
        worker_path = self.file_manager.copy_worker_module(
            worker_source, "coyote_worker.py"
        )

        return worker_path

    def _find_worker_module(self) -> str:
        """Find the coyote_worker.py reference implementation"""
        # Look in common locations
        possible_locations = [
            Path(__file__).parent.parent
            / "workers"
            / "coyote_worker.py",  # Package location
            Path.cwd() / "coyote_worker.py",  # Current directory
            Path(__file__).parent / "coyote_worker.py",  # Same directory as this file
        ]

        for location in possible_locations:
            if location.exists():
                return str(location)

        raise FileNotFoundError(
            "Could not find coyote_worker.py. Please ensure it's in the workers directory "
            "or current working directory."
        )

    def create_parameter_files(self) -> None:
        """Create JSON parameter files for the worker module"""
        # Write common parameters
        with open(self.common_params_file, "w") as f:
            json.dump(self.common_params, f, indent=2)

        # Write app parameters
        with open(self.app_params_file, "w") as f:
            json.dump(self.app_params, f, indent=2)

    def build_worker_command(self, mode: str) -> List[str]:
        """Build command to execute coyote_worker.py"""
        cfcache_dir = self.get_cfcache_path()

        args = {
            "cfcache_dir": cfcache_dir,
            "nprocs": str(self.nprocs),
            "coyote_app": self.coyote_binary,
            "mode": mode,
        }

        # Add parameter files if they exist
        if self.common_params_file.exists():
            args["common_params_file"] = str(self.common_params_file)
        if self.app_params_file.exists():
            args["app_params_file"] = str(self.app_params_file)

        return self.command_builder.build_python_command(
            str(self.worker_module_path), **args
        )

    def _generate_single_job_with_custom_command(
        self, job_name: str, command: List[str], memory_key: str, dependency: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate a single job with a custom command"""
        # Get base job configuration
        job_config = self.get_base_job_config(job_name, memory_key)
        
        # Add dependency if specified
        if dependency:
            job_config["dependency"] = f"afterok:{dependency}"
        
        # Generate script
        script_content = self.script_generator.generate_script(
            job_config, command, ""
        )
        
        # Write script to file
        script_filename = f"{job_name}.sh"
        script_path = self.file_manager.write_script(script_content, script_filename)
        
        return {
            "type": "single",
            "job_name": job_name,
            "script_path": script_path,
            "depends_on": dependency,
        }

    def _generate_array_job_with_custom_command(
        self, job_name: str, command: List[str], memory_key: str, array_range: str, dependency: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate an array job with a custom command"""
        # Get base job configuration
        job_config = self.get_base_job_config(job_name, memory_key)
        
        # Override log paths for array jobs
        output_log, error_log = self.file_manager.get_log_paths(
            job_name, array_job=True
        )
        job_config.update(
            {"output": output_log, "error": error_log, "array_range": array_range}
        )
        
        # Add dependency if specified
        if dependency:
            job_config["dependency"] = f"afterok:{dependency}"
        
        # Generate script
        script_content = self.script_generator.generate_script(
            job_config, command, ""
        )
        
        # Write script to file
        script_filename = f"{job_name}.sh"
        script_path = self.file_manager.write_script(script_content, script_filename)
        
        return {
            "type": "array",
            "job_name": job_name,
            "script_path": script_path,
            "depends_on": dependency,
            "array_range": array_range,
        }

    def generate_dryrun_job(self, dependency: Optional[str] = None) -> Dict[str, Any]:
        """Generate dryrun job (single job)"""
        basename = self.common_params["basename"]
        job_name = f"{basename}_coyote_dryrun"
        
        # For dryrun, we can use the direct coyote command (no worker needed)
        # But for consistency, we'll use the worker module approach
        command = self.build_worker_command("dryrun")
        
        job_info = self._generate_single_job_with_custom_command(
            job_name, command, "coyote_mem", dependency
        )
        
        job_info["phase"] = "dryrun"
        return job_info

    def generate_fillcf_job(self, dependency: Optional[str] = None) -> Dict[str, Any]:
        """Generate fillcf job (array job)"""
        basename = self.common_params["basename"]
        job_name = f"{basename}_coyote_fillcf"
        array_range = f"0-{self.nprocs - 1}"
        
        command = self.build_worker_command("fillcf")
        
        job_info = self._generate_array_job_with_custom_command(
            job_name, command, "coyote_mem", array_range, dependency
        )
        
        job_info["phase"] = "fillcf"
        return job_info

    def generate_jobs(self) -> List[Dict[str, Any]]:
        """Generate both dryrun and fillcf jobs with proper dependencies"""
        # Create worker module and parameter files
        self.create_worker_module()
        self.create_parameter_files()

        jobs = []

        # Generate dryrun job (no dependencies)
        dryrun_job = self.generate_dryrun_job()
        jobs.append(dryrun_job)

        # Generate fillcf job (depends on dryrun)
        # Note: In practice, this would use the actual dryrun job ID
        fillcf_job = self.generate_fillcf_job(dependency="$DRYRUN_JOB_ID")
        fillcf_job["depends_on_job"] = dryrun_job["job_name"]  # For pipeline tracking
        jobs.append(fillcf_job)

        return jobs
