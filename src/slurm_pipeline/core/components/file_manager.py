"""
Composition classes for clean, maintainable SLURM job generation

These classes handle specific aspects of job creation:
- FileManager: Handles file operations and naming
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os
import shutil


class FileManager:
    """Handles file operations and naming conventions"""

    def __init__(self, working_dir: str, basename: str) -> None:
        self.working_dir = Path(working_dir).resolve()
        self.basename = basename
        self.logs_dir = self.working_dir / "logs"
        self.data_dir = self.working_dir / "data"

        # Ensure directories exist
        self.working_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)

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
        source = Path(source_path)
        target = self.working_dir / target_name

        if not source.exists():
            raise FileNotFoundError(f"Worker module not found: {source}")

        shutil.copy2(source, target)
        target.chmod(0o755)

        return str(target)
    
    def setup_data_directory(self, source_data_dir: Optional[str] = None) -> str:
        """
        Set up data directory in working directory
        
        Args:
            source_data_dir: Path to source data directory. If None, uses package data.
        
        Returns:
            Path to data directory for CASAPATH
        """
        if source_data_dir:
            source_data = Path(source_data_dir)
        else:
            # Use package data directory
            package_root = Path(__file__).parent.parent.parent.parent.parent
            source_data = package_root / "data"
        
        if not source_data.exists():
            raise FileNotFoundError(f"Source data directory not found: {source_data}")
        
        # Check for required VLA surface file
        vla_surface_file = source_data / "nrao" / "VLA" / "VLA.surface"
        if not vla_surface_file.exists():
            raise FileNotFoundError(f"Required VLA surface file not found: {vla_surface_file}")
        
        # Copy entire data directory structure
        target_data = self.data_dir
        if target_data.exists():
            shutil.rmtree(target_data)
        
        shutil.copytree(source_data, target_data)
        
        # Verify the copy was successful
        target_vla_surface = target_data / "nrao" / "VLA" / "VLA.surface"
        if not target_vla_surface.exists():
            raise RuntimeError(f"Failed to copy VLA surface file to: {target_vla_surface}")
        
        print(f"Data directory set up at: {target_data}")
        print(f"VLA surface file available at: {target_vla_surface}")
        
        return str(target_data)
    
    def get_casapath(self) -> str:
        """Get CASAPATH environment variable value"""
        return str(self.data_dir)
    
    def validate_data_setup(self) -> bool:
        """
        Validate that data directory is properly set up
        
        Returns:
            True if data setup is valid
        """
        if not self.data_dir.exists():
            return False
        
        vla_surface = self.data_dir / "nrao" / "VLA" / "VLA.surface"
        if not vla_surface.exists():
            return False
            
        return True
