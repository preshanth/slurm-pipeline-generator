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


class GPUJob(BaseJob):
    """GPU-enabled SLURM job implementation"""

    def __init__(self, config_parser: Any, working_dir: str = ".", gpu_count: int = 1) -> None:
        """Initialize GPU job with GPU requirements"""
        super().__init__(config_parser, working_dir)
        self.gpu_count = gpu_count

        # Validate GPU configuration
        if "gpu_type" not in self.slurm_config:
            raise ValueError("GPU job requires 'gpu_type' in SLURM configuration")

    def get_gpu_job_config(
        self, job_name: str, memory_key: str, walltime_key: Optional[str] = None
    ) -> Dict[str, str]:
        """Get GPU-specific job configuration"""
        job_config = self.get_base_job_config(job_name, memory_key, walltime_key)

        # Add GPU resources
        gpu_resources = self.resource_config.get_gpu_resources()
        if gpu_resources:
            job_config["constraint"] = gpu_resources["constraint"]
            job_config["gres"] = f"gpu:{self.gpu_count}"

            # Override memory with GPU-appropriate amount if not specified
            if memory_key not in self.slurm_config:
                job_config["mem"] = gpu_resources["cpu_mem_per_gpu"]

        return job_config

    def generate_gpu_job(
        self,
        job_name: str,
        memory_key: str,
        mode: Optional[str] = None,
        command_args: Optional[Dict] = None,
        dependency: Optional[str] = None,
        walltime_key: Optional[str] = None,
        environment_setup: str = "",
    ) -> Dict[str, Any]:
        """Generate a GPU SLURM job"""

        # Get GPU-specific job configuration
        job_config = self.get_gpu_job_config(job_name, memory_key, walltime_key)

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
            "type": "gpu",
            "job_name": job_name,
            "script_path": script_path,
            "depends_on": dependency,
            "gpu_type": self.slurm_config.get("gpu_type"),
            "gpu_count": self.gpu_count,
            "mode": mode,
        }
