#!/usr/bin/env python3
"""
tests/unit/test_config_parser.py

Unit tests for ConfigParser class
"""

import pytest
import tempfile
import os
from pathlib import Path

from slurm_pipeline.core import ConfigParser


class TestConfigParser:
    """Test ConfigParser functionality"""

    def test_initialization_success(self, sample_def_file):
        """Test successful ConfigParser initialization"""
        parser = ConfigParser(str(sample_def_file))

        assert parser.def_file_path == sample_def_file
        assert parser.config is not None

    def test_initialization_file_not_found(self):
        """Test ConfigParser initialization with non-existent file"""
        with pytest.raises(FileNotFoundError, match="Definition file not found"):
            ConfigParser("/nonexistent/path/test.def")

    def test_missing_required_sections(self, temp_dir):
        """Test ConfigParser with missing required sections"""
        # Create .def file missing [slurm] section
        incomplete_def = """[common]
vis = test.ms
basename = test_run
"""

        def_file = temp_dir / "incomplete.def"
        with open(def_file, "w") as f:
            f.write(incomplete_def)

        with pytest.raises(ValueError, match="Missing required sections"):
            ConfigParser(str(def_file))

    def test_get_common_params(self, sample_def_file):
        """Test getting common parameters"""
        parser = ConfigParser(str(sample_def_file))
        common_params = parser.get_common_params()

        assert isinstance(common_params, dict)
        assert "vis" in common_params
        assert "basename" in common_params
        assert "telescope" in common_params
        assert common_params["vis"] == "test_sim.ms"
        assert common_params["basename"] == "test_run"

    def test_get_slurm_config(self, sample_def_file):
        """Test getting SLURM configuration"""
        parser = ConfigParser(str(sample_def_file))
        slurm_config = parser.get_slurm_config()

        assert isinstance(slurm_config, dict)
        assert "account" in slurm_config
        assert "email" in slurm_config
        assert "gpu_type" in slurm_config
        assert slurm_config["account"] == "test_account"
        assert slurm_config["email"] == "user@msu.edu"
        assert slurm_config["gpu_type"] == "h200"

    def test_get_app_params_existing(self, sample_def_file):
        """Test getting parameters for existing application"""
        parser = ConfigParser(str(sample_def_file))
        coyote_params = parser.get_app_params("coyote")

        assert isinstance(coyote_params, dict)
        assert len(coyote_params) > 0
        assert "wplanes" in coyote_params
        assert "cfcache" in coyote_params
        assert coyote_params["wplanes"] == "1"
        assert coyote_params["cfcache"] == "test.cf"

    def test_get_app_params_nonexistent(self, sample_def_file):
        """Test getting parameters for non-existent application"""
        parser = ConfigParser(str(sample_def_file))
        missing_params = parser.get_app_params("nonexistent")

        assert isinstance(missing_params, dict)
        assert len(missing_params) == 0

    def test_get_gpu_resources_valid_types(self, sample_def_file):
        """Test getting GPU resources for valid GPU types"""
        parser = ConfigParser(str(sample_def_file))

        # Test each supported GPU type
        gpu_types = ["h200", "l40s", "a100", "v100s"]

        for gpu_type in gpu_types:
            gpu_resources = parser.get_gpu_resources(gpu_type)

            assert isinstance(gpu_resources, dict)
            assert "constraint" in gpu_resources
            assert "cpu_mem_per_gpu" in gpu_resources
            assert "gpu_mem" in gpu_resources
            assert "default_gpu_walltime" in gpu_resources
            assert gpu_resources["constraint"] == gpu_type

        # Test specific values for h200
        h200_resources = parser.get_gpu_resources("h200")
        assert h200_resources["gpu_mem"] == "141GB"
        assert h200_resources["cpu_mem_per_gpu"] == "128GB"

    def test_get_gpu_resources_invalid_type(self, sample_def_file):
        """Test getting GPU resources for invalid GPU type"""
        parser = ConfigParser(str(sample_def_file))

        with pytest.raises(ValueError, match="Unsupported GPU type 'invalid_gpu'"):
            parser.get_gpu_resources("invalid_gpu")

    def test_validate_required_params_success(self, sample_def_file):
        """Test successful validation of required parameters"""
        parser = ConfigParser(str(sample_def_file))

        # Should not raise exception
        parser.validate_required_params()

    def test_validate_required_params_missing_common(self, temp_dir):
        """Test validation with missing common parameters"""
        incomplete_def = """[common]
# Missing 'vis' and 'basename'
telescope = EVLA

[slurm]
account = test_account
email = user@msu.edu
"""

        def_file = temp_dir / "incomplete.def"
        with open(def_file, "w") as f:
            f.write(incomplete_def)

        parser = ConfigParser(str(def_file))

        with pytest.raises(ValueError, match="Missing required.*common.*parameters"):
            parser.validate_required_params()

    def test_validate_required_params_missing_slurm(self, temp_dir):
        """Test validation with missing SLURM parameters"""
        incomplete_def = """[common]
vis = test.ms
basename = test_run

[slurm]
# Missing 'account' and 'email'
gpu_type = h200
"""

        def_file = temp_dir / "incomplete.def"
        with open(def_file, "w") as f:
            f.write(incomplete_def)

        parser = ConfigParser(str(def_file))

        with pytest.raises(ValueError, match="Missing required.*slurm.*parameters"):
            parser.validate_required_params()

    def test_get_all_sections(self, sample_def_file):
        """Test getting all section names"""
        parser = ConfigParser(str(sample_def_file))
        sections = parser.get_all_sections()

        assert isinstance(sections, list)
        assert "common" in sections
        assert "slurm" in sections
        assert "coyote" in sections
        assert "roadrunner" in sections
        assert "hummbee" in sections
        assert "dale" in sections

    def test_print_config_summary(self, sample_def_file, capsys):
        """Test printing configuration summary"""
        parser = ConfigParser(str(sample_def_file))
        parser.print_config_summary()

        captured = capsys.readouterr()
        assert "Configuration file:" in captured.out
        assert "Sections found:" in captured.out
        assert "[common] parameters:" in captured.out
        assert "[slurm] configuration:" in captured.out

    def test_print_all_sections(self, sample_def_file, capsys):
        """Test printing all sections"""
        parser = ConfigParser(str(sample_def_file))
        parser.print_all_sections()

        captured = capsys.readouterr()
        assert "Configuration file:" in captured.out
        assert "[common]" in captured.out
        assert "[slurm]" in captured.out
        assert "[coyote]" in captured.out
        assert "vis = test_sim.ms" in captured.out

    def test_empty_sections_handling(self, temp_dir):
        """Test handling of empty sections"""
        def_with_empty_sections = """[common]
vis = test.ms
basename = test_run

[slurm]
account = test_account
email = user@msu.edu

[coyote]
wplanes = 1

[roadrunner]
# Empty section

[empty_section]

[dale]
param1 = value1
"""

        def_file = temp_dir / "empty_sections.def"
        with open(def_file, "w") as f:
            f.write(def_with_empty_sections)

        parser = ConfigParser(str(def_file))

        # Test empty sections
        empty_params = parser.get_app_params("roadrunner")
        assert len(empty_params) == 0

        empty_params2 = parser.get_app_params("empty_section")
        assert len(empty_params2) == 0

        # Test non-empty section
        coyote_params = parser.get_app_params("coyote")
        assert len(coyote_params) == 1
        assert coyote_params["wplanes"] == "1"

    def test_parameter_values_with_special_characters(self, temp_dir):
        """Test handling parameters with special characters and empty values"""
        special_def = """[common]
vis = test file with spaces.ms
basename = test-run_001
phasecenter = 19:59:58.500000 +40.40.00.00000 J2000

[slurm]
account = test_account
email = user@msu.edu

[coyote]
field = 
spw = *
comment = # This is a comment value
equals_in_value = key=value=more
"""

        def_file = temp_dir / "special.def"
        with open(def_file, "w") as f:
            f.write(special_def)

        parser = ConfigParser(str(def_file))

        common_params = parser.get_common_params()
        assert common_params["vis"] == "test file with spaces.ms"
        assert common_params["basename"] == "test-run_001"

        coyote_params = parser.get_app_params("coyote")
        assert coyote_params["field"] == ""  # Empty value
        assert coyote_params["spw"] == "*"
        assert "comment" in coyote_params
        assert "equals_in_value" in coyote_params


class TestConfigParserIntegration:
    """Integration tests for ConfigParser"""

    def test_realistic_pipeline_config(self, temp_dir):
        """Test with a realistic complete pipeline configuration"""
        realistic_def = """[common]
vis = VLASS_J123456+789012.ms
telescope = EVLA
imsize = 2048
cell = 4
stokes = I
reffreq = 3.0GHz
phasecenter = 12:34:56.789 +78:90:12.345 J2000
basename = VLASS_J123456
iterations = 10

[slurm]
account = astronomy_project
email = observer@university.edu
partition = gpu
gpu_type = a100
default_walltime = 8:00:00
coyote_mem = 8GB
coyote_nprocs = 64
roadrunner_mem = 64GB
dale_mem = 32GB
hummbee_mem = 32GB

[coyote]
wplanes = 64
cfcache = VLASS_J123456.cf
wbawp = 1
aterm = 1
psterm = 1
conjbeams = 1
muellertype = full
dpa = 5.0
field = 0
spw = 0:100~900
buffersize = 1000
oversampling = 8

[roadrunner]
gridding_mode = fast
image_weighting = briggs
robust = -0.5

[hummbee]
deconvolver = mtmfs
scales = 0,3,10,30
nterms = 2
gain = 0.05
threshold = 1e-5

[dale]
pblimit = 0.1
normalize_by = peak
"""

        def_file = temp_dir / "realistic.def"
        with open(def_file, "w") as f:
            f.write(realistic_def)

        parser = ConfigParser(str(def_file))

        # Validate it parses correctly
        parser.validate_required_params()

        # Test various parameter retrieval
        common = parser.get_common_params()
        assert common["imsize"] == "2048"
        assert common["reffreq"] == "3.0GHz"

        slurm = parser.get_slurm_config()
        assert slurm["coyote_nprocs"] == "64"
        assert slurm["gpu_type"] == "a100"

        # Test all applications have parameters
        for app in ["coyote", "roadrunner", "hummbee", "dale"]:
            params = parser.get_app_params(app)
            assert len(params) > 0, f"Application {app} should have parameters"

        # Test GPU resources for a100
        gpu_resources = parser.get_gpu_resources("a100")
        assert gpu_resources["gpu_mem"] == "80GB"

        print("Realistic pipeline configuration test passed! ✓")

    def test_minimal_valid_config(self, temp_dir):
        """Test with minimal valid configuration"""
        minimal_def = """[common]
vis = minimal.ms
basename = minimal_test

[slurm]
account = test
email = test@example.com
"""

        def_file = temp_dir / "minimal.def"
        with open(def_file, "w") as f:
            f.write(minimal_def)

        parser = ConfigParser(str(def_file))
        parser.validate_required_params()  # Should not raise

        # All app sections should return empty dicts
        for app in ["coyote", "roadrunner", "hummbee", "dale"]:
            params = parser.get_app_params(app)
            assert len(params) == 0


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
