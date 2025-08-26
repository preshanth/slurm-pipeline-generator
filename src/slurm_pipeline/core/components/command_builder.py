"""
Composition classes for clean, maintainable SLURM job generation

These classes handle specific aspects of job creation:
- CommandBuilder: Constructs commands from parameters
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os


class CommandBuilder:
    """Builds application commands from parameters without string templates"""

    def __init__(self):
        self.executable = None
        self.base_args = {}
        self.mode_args = {}

    def set_executable(self, executable: str):
        """Set the main executable path"""
        self.executable = str(Path(executable).resolve())
        return self

    def add_base_args(self, **kwargs):
        """Add base arguments that apply to all modes"""
        self.base_args.update(kwargs)
        return self

    def add_mode_args(self, mode: str, **kwargs):
        """Add mode-specific arguments"""
        if mode not in self.mode_args:
            self.mode_args[mode] = {}
        self.mode_args[mode].update(kwargs)
        return self

    def build_command(
        self, mode: Optional[str] = None, extra_args: Optional[Dict] = None
    ) -> List[str]:
        """Build command as list of strings"""
        if not self.executable:
            raise ValueError("Executable not set")

        cmd = [self.executable]

        # Add base arguments
        for key, value in self.base_args.items():
            if value is not None and value != "":
                cmd.append(f"{key}={value}")

        # Add mode-specific arguments
        if mode and mode in self.mode_args:
            for key, value in self.mode_args[mode].items():
                if value is not None and value != "":
                    cmd.append(f"{key}={value}")

        # Add extra arguments
        if extra_args:
            for key, value in extra_args.items():
                if value is not None and value != "":
                    cmd.append(f"{key}={value}")

        return cmd

    def build_python_command(self, script_path: str, **kwargs) -> List[str]:
        """Build Python command with --arg value format"""
        cmd = ["python3", str(script_path)]

        for key, value in kwargs.items():
            if value is not None:
                cmd.extend([f"--{key}", str(value)])

        return cmd
