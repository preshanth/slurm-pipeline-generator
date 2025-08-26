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


class BaseJob(ABC):
    """
    Abstract base class for all SLURM jobs
    Uses composition for clean separation of concerns
    """

    def __init__(self, config_parser, working_dir: str = "."):
        """Initialize base job with composition components"""
        self.config = config_parser
        self.common_params = config_parser.get_common_params()
        self.slurm_config = config_parser.get_slurm_config()
        self.app_params = config_parser.get_app_params(self.get_app_name())

        # Composition components
        self.resource_config = ResourceConfig(self.slurm_config)
        self.file_manager = FileManager(working_dir, self.common_params["basename"])
        self.script_generator = ScriptGenerator(self.resource_config, self.file_manager)
        self.command_builder = CommandBuilder()

        # Initialize command builder with app-specific setup
        self.setup_command_builder()

    @abstractmethod
    def get_app_name(self) -> str:
        """Return the application name for parameter lookup"""
        pass

    @abstractmethod
    def setup_command_builder(self):
        """Setup application-specific command builder configuration"""
        pass

    @abstractmethod
    def generate_jobs(self) -> List[Dict[str, Any]]:
        """Generate all job configurations for this application"""
        pass

    def validate_requirements(self):
        """Validate that all required parameters are present"""
        # Base validation - can be extended by subclasses
        required_common = ["vis", "basename"]
        missing_common = [p for p in required_common if p not in self.common_params]

        required_slurm = ["account", "email"]
        missing_slurm = [p for p in required_slurm if p not in self.slurm_config]

        if missing_common or missing_slurm:
            missing = missing_common + missing_slurm
            raise ValueError(f"Missing required parameters: {missing}")

    def get_base_job_config(
        self, job_name: str, memory_key: str, walltime_key: Optional[str] = None
    ) -> Dict[str, str]:
        """Get base SLURM job configuration"""
        output_log, error_log = self.file_manager.get_log_paths(job_name)

        return {
            "job_name": job_name,
            "time": self.resource_config.get_walltime(walltime_key),
            "mem": self.resource_config.get_memory(memory_key),
            "account": self.slurm_config["account"],
            "mail_user": self.slurm_config["email"],
            "mail_type": "FAIL",
            "output": output_log,
            "error": error_log,
            "nodes": "1",
            "ntasks_per_node": "1",
        }
