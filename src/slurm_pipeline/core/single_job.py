
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

from .base_job import BaseJob
from .components import CommandBuilder, ResourceConfig, FileManager, ScriptGenerator

class SingleJob(BaseJob):
    """Single SLURM job implementation"""
    
    def generate_single_job(self, job_name: str, memory_key: str, mode: Optional[str] = None,
                           command_args: Optional[Dict] = None, dependency: Optional[str] = None,
                           walltime_key: Optional[str] = None, environment_setup: str = "") -> Dict[str, Any]:
        """Generate a single SLURM job"""
        
        # Get base job configuration
        job_config = self.get_base_job_config(job_name, memory_key, walltime_key)
        
        # Add dependency if specified
        if dependency:
            job_config['dependency'] = f"afterok:{dependency}"
        
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
            'type': 'single',
            'job_name': job_name,
            'script_path': script_path,
            'depends_on': dependency,
            'mode': mode
        }
