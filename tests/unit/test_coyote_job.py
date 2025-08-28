# tests/unit/test_coyote_job.py
"""
Unit tests for CoyoteJob
"""

import pytest
import json
import os
import shutil
from pathlib import Path

from slurm_pipeline.applications import CoyoteJob


@pytest.fixture(autouse=True)
def setup_vla_surface_file(temp_dir):
    """Fixture to set CASAPATH and ensure VLA.surface is present in the test data directory."""
    # Set CASAPATH to the temp data directory
    casa_data_dir = Path(temp_dir) / "data"
    os.environ["CASAPATH"] = str(casa_data_dir)
    # Copy the VLA.surface file
    vla_surface_src = Path("data/nrao/VLA/VLA.surface")
    vla_surface_dst = casa_data_dir / "nrao" / "VLA"
    vla_surface_dst.mkdir(parents=True, exist_ok=True)
    if vla_surface_src.exists():
        shutil.copy(vla_surface_src, vla_surface_dst / "VLA.surface")
        # Debug prints to help diagnose test failures
        print("VLA.surface exists:", (vla_surface_dst / "VLA.surface").exists(), "at", vla_surface_dst / "VLA.surface")
        print("CASAPATH is set to:", os.environ["CASAPATH"])
    yield
    # Optionally, cleanup can be added here if needed


class TestCoyoteJob:
    """Test CoyoteJob functionality"""

    def test_coyote_job_initialization(self, temp_dir, sample_def_file, mock_data_dir):
        """Test CoyoteJob initialization"""
        from slurm_pipeline.core import ConfigParser
        from unittest.mock import patch

        config = ConfigParser(str(sample_def_file))

        # Create dummy coyote binary
        coyote_binary = temp_dir / "coyote"
        coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote'")
        coyote_binary.chmod(0o755)

        # Mock the setup_data_directory to use our mock data directory
        with patch('slurm_pipeline.core.components.file_manager.FileManager.setup_data_directory') as mock_setup:
            mock_setup.return_value = mock_data_dir
            coyote_job = CoyoteJob(config, str(temp_dir), str(coyote_binary))

        assert coyote_job.get_app_name() == "coyote"
        assert coyote_job.nprocs == 8  # From sample_def_content
        assert Path(coyote_job.coyote_binary).exists()

    def test_cfcache_path(self, temp_dir, sample_def_file, mock_data_dir):
        """Test CF cache path generation"""
        from slurm_pipeline.core import ConfigParser
        from unittest.mock import patch

        config = ConfigParser(str(sample_def_file))

        coyote_binary = temp_dir / "coyote"
        coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote'")
        coyote_binary.chmod(0o755)

        # Mock the setup_data_directory to use our mock data directory
        with patch('slurm_pipeline.core.components.file_manager.FileManager.setup_data_directory') as mock_setup:
            mock_setup.return_value = mock_data_dir
            coyote_job = CoyoteJob(config, str(temp_dir), str(coyote_binary))
        cfcache_path = coyote_job.get_cfcache_path()

        # Should be relative to working directory
        assert str(temp_dir) in cfcache_path
        assert "test.cf" in cfcache_path

    def test_parameter_files_creation(self, temp_dir, sample_def_file, mock_data_dir):
        """Test parameter file creation"""
        from slurm_pipeline.core import ConfigParser
        from unittest.mock import patch

        config = ConfigParser(str(sample_def_file))

        coyote_binary = temp_dir / "coyote"
        coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote'")
        coyote_binary.chmod(0o755)

        # Mock the setup_data_directory to use our mock data directory
        with patch('slurm_pipeline.core.components.file_manager.FileManager.setup_data_directory') as mock_setup:
            mock_setup.return_value = mock_data_dir
            coyote_job = CoyoteJob(config, str(temp_dir), str(coyote_binary))
        coyote_job.create_parameter_files()

        # Check files exist
        assert coyote_job.common_params_file.exists()
        assert coyote_job.app_params_file.exists()

        # Check content
        with open(coyote_job.common_params_file, "r") as f:
            common_params = json.load(f)
            assert "basename" in common_params
            assert "vis" in common_params

        # Debug: print the paths of the created parameter files
        print("Common params file created at:", coyote_job.common_params_file)
        print("App params file created at:", coyote_job.app_params_file)

        # Additional checks can be added here depending on the expected content
        with open(coyote_job.app_params_file, "r") as f:
            app_params = json.load(f)
        assert "cfcache" in app_params

    def test_vla_surface_file_exists(self, temp_dir):
        """Test that the VLA.surface file exists in the expected location"""
        vla_surface_path = Path(os.environ["CASAPATH"]) / "nrao" / "VLA" / "VLA.surface"
        print("Checking for VLA.surface at:", vla_surface_path)
        print("File exists:", os.path.exists(vla_surface_path))
        print("DEBUG: Looking for VLA.surface at:", vla_surface_path)
        print("DEBUG: File exists:", os.path.exists(vla_surface_path))
        assert vla_surface_path.exists()
