"""
SLURM Pipeline Generator

A Python package for generating SLURM job scripts for radio astronomy pipelines.
"""

__version__ = "0.1.0"

from .core import ConfigParser, PipelineDriver
from .applications import CoyoteJob

__all__ = ["ConfigParser", "PipelineDriver", "CoyoteJob"]
