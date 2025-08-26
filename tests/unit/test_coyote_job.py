
# tests/unit/test_coyote_job.py
"""
Unit tests for CoyoteJob
"""

import pytest
import json
from pathlib import Path

from slurm_pipeline.applications import CoyoteJob


class TestCoyoteJob:
    """Test CoyoteJob functionality"""
    
    def test_coyote_job_initialization(self, temp_dir, sample_def_file):
        """Test CoyoteJob initialization"""
        from slurm_pipeline.core import ConfigParser
        
        config = ConfigParser(str(sample_def_file))
        
        # Create dummy coyote binary
        coyote_binary = temp_dir / "coyote"
        coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote'")
        coyote_binary.chmod(0o755)
        
        coyote_job = CoyoteJob(config, str(temp_dir), str(coyote_binary))
        
        assert coyote_job.get_app_name() == "coyote"
        assert coyote_job.nprocs == 8  # From sample_def_content
        assert Path(coyote_job.coyote_binary).exists()
    
    def test_cfcache_path(self, temp_dir, sample_def_file):
        """Test CF cache path generation"""
        from slurm_pipeline.core import ConfigParser
        
        config = ConfigParser(str(sample_def_file))
        
        coyote_binary = temp_dir / "coyote"
        coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote'")
        coyote_binary.chmod(0o755)
        
        coyote_job = CoyoteJob(config, str(temp_dir), str(coyote_binary))
        cfcache_path = coyote_job.get_cfcache_path()
        
        # Should be relative to working directory
        assert str(temp_dir) in cfcache_path
        assert "test.cf" in cfcache_path
    
    def test_parameter_files_creation(self, temp_dir, sample_def_file):
        """Test parameter file creation"""
        from slurm_pipeline.core import ConfigParser
        
        config = ConfigParser(str(sample_def_file))
        
        coyote_binary = temp_dir / "coyote"
        coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote'")
        coyote_binary.chmod(0o755)
        
        coyote_job = CoyoteJob(config, str(temp_dir), str(coyote_binary))
        coyote_job.create_parameter_files()
        
        # Check files exist
        assert coyote_job.common_params_file.exists()
        assert coyote_job.app_params_file.exists()
        
        # Check content
        with open(coyote_job.common_params_file, 'r') as f:
            common_params = json.load(f)
            assert 'basename' in common_params
            assert 'vis' in common_params
