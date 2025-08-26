#!/usr/bin/env python3
"""
src/slurm_pipeline/core/config_parser.py

Configuration file parser for .def files
Uses Python's built-in configparser for robust .ini-style file handling
"""

import configparser
import os
from typing import Dict, Optional
from pathlib import Path


class ConfigParser:
    """Parser for .def configuration files"""

    def __init__(self, def_file_path: str):
        """
        Initialize parser with .def file path

        Args:
            def_file_path: Path to the .def configuration file

        Raises:
            FileNotFoundError: If .def file doesn't exist
            ValueError: If required sections are missing
        """
        self.def_file_path = Path(def_file_path).resolve()

        if not self.def_file_path.exists():
            raise FileNotFoundError(f"Definition file not found: {self.def_file_path}")

        self.config = configparser.ConfigParser(interpolation=None)
        self.config.read(str(self.def_file_path))

        # Validate required sections exist
        self.validate_required_sections()

    def validate_required_sections(self):
        """Validate that required sections exist in the .def file"""
        required_sections = ["common", "slurm"]
        missing_sections = [
            sec for sec in required_sections if not self.config.has_section(sec)
        ]

        if missing_sections:
            raise ValueError(
                f"Missing required sections in {self.def_file_path}: {missing_sections}"
            )

    def get_common_params(self) -> Dict[str, str]:
        """Get parameters from [common] section"""
        return dict(self.config["common"])

    def get_slurm_config(self) -> Dict[str, str]:
        """Get SLURM configuration from [slurm] section"""
        return dict(self.config["slurm"])

    def get_app_params(self, app_name: str) -> Dict[str, str]:
        """Get parameters for specific application, returns empty dict if section missing"""
        if self.config.has_section(app_name):
            return dict(self.config[app_name])
        return {}

    def get_gpu_resources(self, gpu_type: str) -> Dict[str, str]:
        """
        Map GPU type to resource specifications

        Args:
            gpu_type: Type of GPU (h200, l40s, a100, v100s)

        Returns:
            Dictionary with GPU resource specifications

        Raises:
            ValueError: If GPU type is not supported
        """
        gpu_resources = {
            "h200": {
                "constraint": "h200",
                "cpu_mem_per_gpu": "128GB",
                "gpu_mem": "141GB",
                "default_gpu_walltime": "1-00:00:00",
            },
            "l40s": {
                "constraint": "l40s",
                "cpu_mem_per_gpu": "64GB",
                "gpu_mem": "48GB",
                "default_gpu_walltime": "1-00:00:00",
            },
            "a100": {
                "constraint": "a100",
                "cpu_mem_per_gpu": "64GB",
                "gpu_mem": "80GB",
                "default_gpu_walltime": "1-00:00:00",
            },
            "v100s": {
                "constraint": "v100s",
                "cpu_mem_per_gpu": "32GB",
                "gpu_mem": "32GB",
                "default_gpu_walltime": "1-00:00:00",
            },
        }

        if gpu_type not in gpu_resources:
            available_types = list(gpu_resources.keys())
            raise ValueError(
                f"Unsupported GPU type '{gpu_type}'. Available: {available_types}"
            )

        return gpu_resources[gpu_type].copy()

    def validate_required_params(self):
        """Validate that essential parameters exist"""
        common_params = self.get_common_params()
        slurm_config = self.get_slurm_config()

        # Required common parameters
        required_common = ["vis", "basename"]
        missing_common = [
            param for param in required_common if param not in common_params
        ]

        # Required SLURM parameters
        required_slurm = ["account", "email"]
        missing_slurm = [param for param in required_slurm if param not in slurm_config]

        errors = []
        if missing_common:
            errors.append(f"Missing required [common] parameters: {missing_common}")
        if missing_slurm:
            errors.append(f"Missing required [slurm] parameters: {missing_slurm}")

        if errors:
            raise ValueError(". ".join(errors))

    def get_all_sections(self) -> list:
        """Get list of all sections in the .def file"""
        return list(self.config.sections())

    def print_config_summary(self):
        """Print a summary of the parsed configuration"""
        print(f"Configuration file: {self.def_file_path}")
        print(f"Sections found: {self.get_all_sections()}")
        print("\n[common] parameters:")
        for key, value in self.get_common_params().items():
            print(f"  {key} = {value}")
        print("\n[slurm] configuration:")
        for key, value in self.get_slurm_config().items():
            print(f"  {key} = {value}")

    def print_all_sections(self):
        """Print all sections in a formatted way for debugging"""
        print(f"=== Configuration file: {self.def_file_path} ===")
        print(f"Sections found: {self.get_all_sections()}\n")

        for section_name in self.config.sections():
            print(f"[{section_name}]")
            section_params = dict(self.config[section_name])
            if section_params:
                for key, value in section_params.items():
                    print(f"  {key} = {value}")
            else:
                print("  (empty section)")
            print()  # blank line between sections


def test_config_parser():
    """Test function for ConfigParser"""
    import tempfile

    # Create a sample .def file for testing
    sample_def = """[common]
vis = sim_VLA.ms
telescope = EVLA
imsize = 512
cell = 12
stokes = I
reffreq = 1.4GHz
phasecenter = 19:59:58.500000 +40.40.00.00000 J2000
basename = test_run
iterations = 3

[slurm]
account = test_account
email = user@msu.edu
partition = gpu
gpu_type = h200
default_walltime = 4:00:00
coyote_mem = 4GB
coyote_nprocs = 40
roadrunner_mem = 32GB
dale_mem = 16GB
hummbee_mem = 16GB

[coyote]
wplanes = 1
cfcache = ps.cf
wbawp = 1
aterm = 0
psterm = 1
conjbeams = 1
muellertype = diagonal
dpa = 360
field = 
spw = *
buffersize = 0
oversampling = 20

[roadrunner]
# Will be implemented later

[hummbee]
# Will be implemented later

[dale]
# Will be implemented later
"""

    with tempfile.NamedTemporaryFile(mode="w", suffix=".def", delete=False) as f:
        f.write(sample_def)
        def_file_path = f.name

    try:
        # Test parser
        parser = ConfigParser(def_file_path)
        parser.validate_required_params()

        print("=== ConfigParser Test Results ===\n")

        # Test all sections
        parser.print_all_sections()

        # Test specific functionality
        print("=== Functionality Tests ===")

        # Test GPU resources
        gpu_resources = parser.get_gpu_resources("h200")
        print(
            f"H200 GPU resources: {gpu_resources['constraint']}, {gpu_resources['gpu_mem']}"
        )

        # Test app params
        coyote_params = parser.get_app_params("coyote")
        print(f"Coyote params count: {len(coyote_params)}")

        # Test missing app params
        missing_params = parser.get_app_params("nonexistent")
        print(f"Missing app params: {missing_params}")

        print("\nConfigParser test passed! ✓")

    except Exception as e:
        print(f"ConfigParser test failed: {e}")
        raise

    finally:
        # Clean up test file
        os.unlink(def_file_path)


if __name__ == "__main__":
    test_config_parser()
