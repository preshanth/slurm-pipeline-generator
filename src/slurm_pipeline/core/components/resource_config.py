"""
Composition classes for clean, maintainable SLURM job generation

These classes handle specific aspects of job creation:
- ResourceConfig: Manages SLURM resource requirements  
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import os

class ResourceConfig:
    """Manages SLURM resource requirements and constraints"""
    
    # GPU resource mapping
    GPU_RESOURCES = {
        'h200': {
            'constraint': 'h200',
            'cpu_mem_per_gpu': '128GB', 
            'gpu_mem': '141GB',
            'default_walltime': '1-00:00:00'
        },
        'l40s': {
            'constraint': 'l40s',
            'cpu_mem_per_gpu': '64GB',
            'gpu_mem': '48GB', 
            'default_walltime': '1-00:00:00'
        },
        'a100': {
            'constraint': 'a100',
            'cpu_mem_per_gpu': '64GB',
            'gpu_mem': '80GB',
            'default_walltime': '1-00:00:00'
        },
        'v100s': {
            'constraint': 'v100s', 
            'cpu_mem_per_gpu': '32GB',
            'gpu_mem': '32GB',
            'default_walltime': '1-00:00:00'
        }
    }
    
    def __init__(self, slurm_config: Dict[str, str]):
        self.slurm_config = slurm_config
        self._resource_cache = {}
    
    def get_memory(self, memory_key: str, default: str = "8GB") -> str:
        """Get memory requirement with fallback"""
        return self.slurm_config.get(memory_key, default)
    
    def get_walltime(self, walltime_key: Optional[str] = None) -> str:
        """Get walltime with fallback to default"""
        if walltime_key and walltime_key in self.slurm_config:
            return self.slurm_config[walltime_key]
        return self.slurm_config.get('default_walltime', '4:00:00')
    
    def get_gpu_resources(self, gpu_type: Optional[str] = None) -> Dict[str, str]:
        """Get GPU resource specifications"""
        if not gpu_type:
            gpu_type = self.slurm_config.get('gpu_type')
        
        if not gpu_type or gpu_type not in self.GPU_RESOURCES:
            return {}
        
        return self.GPU_RESOURCES[gpu_type].copy()
    
    def build_slurm_directives(self, **overrides) -> Dict[str, str]:
        """Build SLURM directive parameters"""
        directives = {
            'account': self.slurm_config.get('account', ''),
            'email': self.slurm_config.get('email', ''),
            'partition': self.slurm_config.get('partition', ''),
            'nodes': '1',
            'ntasks_per_node': '1',
            'export': 'ALL'
        }
        
        # Apply overrides
        directives.update(overrides)
        
        return directives