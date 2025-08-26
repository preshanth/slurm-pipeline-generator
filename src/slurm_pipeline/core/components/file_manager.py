"""
Composition classes for clean, maintainable SLURM job generation

These classes handle specific aspects of job creation:
- FileManager: Handles file operations and naming
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os


class FileManager:
    """Handles file operations and naming conventions"""

    def __init__(self, working_dir: str, basename: str) -> None:
        self.working_dir = Path(working_dir).resolve()
        self.basename = basename
        self.logs_dir = self.working_dir / "logs"

        # Ensure directories exist
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)

    def get_iteration_filename(self, filetype: str, iteration: int = 0) -> str:
        """Generate filename with iteration number"""
        if iteration > 0:
            return f"{self.basename}_iter{iteration:03d}.{filetype}"
        return f"{self.basename}.{filetype}"

    def get_log_paths(
        self, job_prefix: str, array_job: bool = False
    ) -> Tuple[str, str]:
        """Generate log file paths"""
        if array_job:
            output_log = str(self.logs_dir / f"{job_prefix}_%A_%a.out")
            error_log = str(self.logs_dir / f"{job_prefix}_%A_%a.err")
        else:
            output_log = str(self.logs_dir / f"{job_prefix}_%j.out")
            error_log = str(self.logs_dir / f"{job_prefix}_%j.err")

        return output_log, error_log

    def write_script(self, content: str, filename: str) -> str:
        """Write script to file and make executable"""
        script_path = self.working_dir / filename

        with open(script_path, "w") as f:
            f.write(content)

        script_path.chmod(0o755)
        return str(script_path)

    def copy_worker_module(self, source_path: str, target_name: str) -> str:
        """Copy worker module to working directory"""
        import shutil

        source = Path(source_path)
        target = self.working_dir / target_name

        if not source.exists():
            raise FileNotFoundError(f"Worker module not found: {source}")

        shutil.copy2(source, target)
        target.chmod(0o755)

        return str(target)
