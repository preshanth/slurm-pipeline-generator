"""
Inheritable base job classes using composition

Clean hierarchy:
BaseJob (abstract) - defines interface, uses composition
├── SingleJob - single SLURM jobs
├── ArrayJob - array SLURM jobs
├── GPUJob - GPU-enabled jobs
└── GPUArrayJob - GPU array jobs (multiple inheritance)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
from .components import CommandBuilder, ResourceConfig, FileManager, ScriptGenerator
from .base_job import BaseJob


class GPUArrayJob(ArrayJob, GPUJob):
    """GPU-enabled array job using multiple inheritance"""

    def __init__(self, config_parser, working_dir: str = ".", gpu_count: int = 1):
        """Initialize GPU array job - calls both parent constructors"""
        # Initialize ArrayJob first (which calls BaseJob.__init__)
        ArrayJob.__init__(self, config_parser, working_dir)
        # Then initialize GPU-specific attributes
        self.gpu_count = gpu_count

        # Validate GPU configuration (from GPUJob)
        if "gpu_type" not in self.slurm_config:
            raise ValueError("GPU array job requires 'gpu_type' in SLURM configuration")

    def generate_gpu_array_job(
        self,
        job_name: str,
        memory_key: str,
        array_range: str,
        mode: Optional[str] = None,
        command_args: Optional[Dict] = None,
        dependency: Optional[str] = None,
        walltime_key: Optional[str] = None,
        environment_setup: str = "",
    ) -> Dict[str, Any]:
        """Generate a GPU array SLURM job"""

        # Get GPU-specific job configuration
        job_config = self.get_gpu_job_config(job_name, memory_key, walltime_key)

        # Override log paths for array jobs (from ArrayJob)
        output_log, error_log = self.file_manager.get_log_paths(
            job_name, array_job=True
        )
        job_config.update(
            {"output": output_log, "error": error_log, "array_range": array_range}
        )

        # Add dependency if specified
        if dependency:
            job_config["dependency"] = f"afterok:{dependency}"

        # Build command
        command = self.command_builder.build_command(mode, command_args)

        # Generate script
        script_content = self.script_generator.generate_script(
            job_config, command, environment_setup
        )

        # Write script to file
        script_filename = f"{job_name}.sh"
        script_path = self.file_manager.write_script(script_content, script_filename)

        return {
            "type": "gpu_array",
            "job_name": job_name,
            "script_path": script_path,
            "depends_on": dependency,
            "array_range": array_range,
            "gpu_type": self.slurm_config.get("gpu_type"),
            "gpu_count": self.gpu_count,
            "mode": mode,
        }

    # Inherit get_gpu_job_config from GPUJob
    def get_gpu_job_config(
        self, job_name: str, memory_key: str, walltime_key: Optional[str] = None
    ) -> Dict[str, str]:
        """Use GPU job configuration method"""
        return GPUJob.get_gpu_job_config(self, job_name, memory_key, walltime_key)
