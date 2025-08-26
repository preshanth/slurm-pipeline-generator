"""
Composition classes for clean, maintainable SLURM job generation

These classes handle specific aspects of job creation:
- ScriptGenerator: Creates SLURM scripts from components
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os
from .resource_config import ResourceConfig
from .file_manager import FileManager


class ScriptGenerator:
    """Generates SLURM scripts from components"""

    # Base SLURM script template
    SLURM_TEMPLATE = """#!/bin/bash
{directives}

{environment_setup}

{command}
"""

    def __init__(self, resource_config: ResourceConfig, file_manager: FileManager):
        self.resource_config = resource_config
        self.file_manager = file_manager

    def generate_slurm_directives(self, job_config: Dict[str, str]) -> str:
        """Generate SLURM directive lines"""
        directives = []
        # Standard directives
        standard_keys = [
            "export",
            "chdir",
            "time",
            "mem",
            "nodes",
            "ntasks_per_node",
            "output",
            "error",
            "job_name",
            "account",
            "mail_user",
            "mail_type",
        ]

        for key in standard_keys:
            if key in job_config and job_config[key]:
                directive_key = key.replace("_", "-")  # mail_user -> mail-user
                directives.append(f"#SBATCH --{directive_key}={job_config[key]}")

        # Special directives
        if "array_range" in job_config:
            directives.append(f"#SBATCH --array={job_config['array_range']}")

        if "dependency" in job_config and job_config["dependency"]:
            directives.append(f"#SBATCH --dependency={job_config['dependency']}")

        if "constraint" in job_config:
            directives.append(f"#SBATCH --constraint={job_config['constraint']}")

        if "gres" in job_config:
            directives.append(f"#SBATCH --gres={job_config['gres']}")

        return "\n".join(directives)

    def generate_script(
        self,
        job_config: Dict[str, str],
        command: List[str],
        environment_setup: str = "",
    ) -> str:
        """Generate complete SLURM script"""

        # Add working directory to job config
        job_config["chdir"] = str(self.file_manager.working_dir)

        # Generate directives
        directives = self.generate_slurm_directives(job_config)

        # Build command string
        command_str = " ".join(command)

        # Generate script
        script = self.SLURM_TEMPLATE.format(
            directives=directives,
            environment_setup=environment_setup,
            command=command_str,
        )

        return script
