"""
pytest configuration and fixtures
"""

import pytest
import tempfile
import os
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_def_content():
    """Sample .def file content for tests"""
    return """[common]
vis = test_sim.ms
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
coyote_nprocs = 8
default_walltime = 3:00:00
coyote_mem = 4GB
roadrunner_mem = 32GB
gpu_type = h200

[coyote]
wplanes = 1
cfcache = test.cf
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

[hummbee]

[dale]
"""


@pytest.fixture
def sample_def_file(temp_dir, sample_def_content):
    """Create a sample .def file for tests"""
    def_file = temp_dir / "test.def"
    with open(def_file, "w") as f:
        f.write(sample_def_content)
    return def_file


@pytest.fixture
def mock_data_dir(temp_dir):
    """Create a mock data directory with required VLA surface file for tests"""
    data_dir = temp_dir / "data"
    vla_dir = data_dir / "nrao" / "VLA"
    vla_dir.mkdir(parents=True, exist_ok=True)
    
    # Create the required VLA surface file
    vla_surface = vla_dir / "VLA.surface"
    vla_surface.write_text("# Mock VLA surface file for testing\n")
    
    return str(data_dir)
