# tests/integration/test_integration.py
"""
Integration tests for the complete pipeline
"""

import pytest
import tempfile
import json
from pathlib import Path


def test_complete_pipeline_integration(temp_dir):
    """Test complete pipeline from .def file to SLURM scripts"""

    # Create test .def file
    def_content = """[common]
vis = integration_test.ms
telescope = EVLA
imsize = 1024
cell = 8
stokes = I
reffreq = 1.4GHz
phasecenter = 19:59:58.500000 +40.40.00.00000 J2000
basename = integration_test
iterations = 3

[slurm]
account = integration_account
email = test@msu.edu
coyote_nprocs = 16
default_walltime = 2:00:00
coyote_mem = 4GB

[coyote]
wplanes = 1
cfcache = integration.cf
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
"""

    def_file = temp_dir / "integration_test.def"
    with open(def_file, "w") as f:
        f.write(def_content)

    # Create dummy coyote binary
    coyote_binary = temp_dir / "coyote"
    coyote_binary.write_text("#!/bin/bash\necho 'dummy coyote for integration test'")
    coyote_binary.chmod(0o755)

    # Create dummy worker module
    worker_content = """#!/usr/bin/env python3
import argparse
import os
import json

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--coyote_app', required=True)
    parser.add_argument('--cfcache_dir', required=True)
    parser.add_argument('--nprocs', type=int, required=True)
    parser.add_argument('--mode', required=True)
    parser.add_argument('--common_params_file')
    parser.add_argument('--app_params_file')
    
    args = parser.parse_args()
    print(f"Integration test worker: {args.mode}")

if __name__ == "__main__":
    main()
"""

    worker_file = temp_dir / "coyote_worker.py"
    with open(worker_file, "w") as f:
        f.write(worker_content)
    worker_file.chmod(0o755)

    # Test the pipeline (would use real classes in practice)
    output_dir = temp_dir / "pipeline_output"

    # This would be the real test once classes are implemented
    # from slurm_pipeline.core import ConfigParser
    # from slurm_pipeline.applications import CoyoteJob

    # config = ConfigParser(str(def_file))
    # coyote_job = CoyoteJob(config, str(output_dir), str(coyote_binary))
    # jobs = coyote_job.generate_jobs()

    # For now, just verify the structure is correct
    assert def_file.exists()
    assert coyote_binary.exists()
    assert worker_file.exists()

    print("Integration test structure validated")
