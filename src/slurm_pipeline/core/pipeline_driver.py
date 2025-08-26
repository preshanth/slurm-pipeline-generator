#!/usr/bin/env python3
"""
src/slurm_pipeline/core/pipeline_driver.py

Pipeline orchestration and job management
Coordinates multiple applications and manages job dependencies
"""

import subprocess
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from .config_parser import ConfigParser


class PipelineDriver:
    """
    Orchestrates the complete pipeline workflow
    Manages multiple applications and their job dependencies
    """

    def __init__(self, config_parser: ConfigParser, working_dir: str = "."):
        """
        Initialize pipeline driver

        Args:
            config_parser: ConfigParser instance with pipeline configuration
            working_dir: Working directory for pipeline execution
        """
        self.config = config_parser
        self.working_dir = Path(working_dir).resolve()
        self.working_dir.mkdir(parents=True, exist_ok=True)

        # Job registry
        self.applications = {}  # name -> job_instance
        self.job_scripts = {}  # job_name -> script_info
        self.job_dependencies = {}  # job_name -> [dependencies]

        # Execution state
        self.submitted_jobs = {}  # job_name -> slurm_job_id

    def add_application(self, name: str, job_instance):
        """
        Add an application job to the pipeline

        Args:
            name: Name for this application (e.g., 'coyote', 'roadrunner')
            job_instance: Instance of job class (CoyoteJob, RoadrunnerJob, etc.)
        """
        self.applications[name] = job_instance

    def generate_all_scripts(
        self, dry_run: bool = True
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate SLURM scripts for all applications

        Args:
            dry_run: If True, only generate scripts without dependencies resolution

        Returns:
            Dictionary mapping application names to their generated job info
        """
        all_generated_jobs = {}

        for app_name, job_instance in self.applications.items():
            print(f"Generating scripts for {app_name}...")

            # Generate jobs for this application
            jobs = job_instance.generate_jobs()
            all_generated_jobs[app_name] = jobs

            # Register jobs and their dependencies
            for job in jobs:
                job_name = job["job_name"]
                self.job_scripts[job_name] = job

                # Track dependencies within this application
                if "depends_on_job" in job:
                    dep_job_name = job["depends_on_job"]
                    if job_name not in self.job_dependencies:
                        self.job_dependencies[job_name] = []
                    self.job_dependencies[job_name].append(dep_job_name)

        if not dry_run:
            self._resolve_cross_application_dependencies()

        return all_generated_jobs

    def _resolve_cross_application_dependencies(self):
        """Resolve dependencies between different applications"""
        # This would implement logic for cross-application dependencies
        # For example: roadrunner depends on coyote fillcf completion

        # Example logic (to be customized based on pipeline requirements):
        common_params = self.config.get_common_params()
        basename = common_params.get("basename", "pipeline")

        # If we have both coyote and roadrunner, roadrunner should depend on coyote fillcf
        coyote_fillcf_job = f"{basename}_coyote_fillcf"
        roadrunner_jobs = [
            name for name in self.job_scripts.keys() if "roadrunner" in name
        ]

        for roadrunner_job in roadrunner_jobs:
            if coyote_fillcf_job in self.job_scripts:
                if roadrunner_job not in self.job_dependencies:
                    self.job_dependencies[roadrunner_job] = []
                self.job_dependencies[roadrunner_job].append(coyote_fillcf_job)

    def print_pipeline_summary(self):
        """Print summary of the pipeline configuration"""
        print("=== Pipeline Summary ===")
        print(f"Working directory: {self.working_dir}")
        print(f"Applications: {len(self.applications)}")

        for app_name in self.applications.keys():
            print(f"  - {app_name}")

        print(f"\nGenerated jobs: {len(self.job_scripts)}")
        for job_name, job_info in self.job_scripts.items():
            job_type = job_info.get("type", "unknown")
            phase = job_info.get("phase", "")
            deps = self.job_dependencies.get(job_name, [])

            print(f"  - {job_name} ({job_type})")
            if phase:
                print(f"    Phase: {phase}")
            if deps:
                print(f"    Depends on: {', '.join(deps)}")

    def validate_pipeline(self) -> Tuple[bool, List[str]]:
        """
        Validate pipeline configuration and dependencies

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check that all applications have valid configurations
        for app_name, job_instance in self.applications.items():
            try:
                job_instance.validate_requirements()
            except Exception as e:
                errors.append(f"{app_name}: {str(e)}")

        # Check for circular dependencies
        visited = set()
        rec_stack = set()

        def has_cycle(job_name):
            if job_name in rec_stack:
                return True
            if job_name in visited:
                return False

            visited.add(job_name)
            rec_stack.add(job_name)

            for dep in self.job_dependencies.get(job_name, []):
                if has_cycle(dep):
                    return True

            rec_stack.remove(job_name)
            return False

        for job_name in self.job_scripts.keys():
            if has_cycle(job_name):
                errors.append(f"Circular dependency detected involving job: {job_name}")
                break

        return len(errors) == 0, errors

    def submit_pipeline(self, dry_run: bool = False) -> Dict[str, str]:
        """
        Submit all pipeline jobs to SLURM with proper dependencies

        Args:
            dry_run: If True, use --test-only flag for sbatch

        Returns:
            Dictionary mapping job names to SLURM job IDs
        """
        if not self.job_scripts:
            raise RuntimeError("No jobs generated. Call generate_all_scripts() first.")

        # Validate pipeline before submission
        is_valid, errors = self.validate_pipeline()
        if not is_valid:
            raise RuntimeError(f"Pipeline validation failed: {'; '.join(errors)}")

        submitted_jobs = {}

        # Submit jobs in dependency order
        submission_order = self._get_submission_order()

        for job_name in submission_order:
            job_info = self.job_scripts[job_name]
            script_path = job_info["script_path"]

            # Build sbatch command
            sbatch_cmd = ["sbatch"]

            if dry_run:
                sbatch_cmd.append("--test-only")

            # Add dependencies if needed
            deps = self.job_dependencies.get(job_name, [])
            resolved_deps = []
            for dep in deps:
                if dep in submitted_jobs:
                    resolved_deps.append(submitted_jobs[dep])

            if resolved_deps:
                dep_string = ":".join(resolved_deps)
                sbatch_cmd.extend(["--dependency", f"afterok:{dep_string}"])

            sbatch_cmd.append(script_path)

            # Submit job
            print(f"Submitting {job_name}...")
            if dry_run:
                print(f"  Command: {' '.join(sbatch_cmd)}")

            try:
                result = subprocess.run(
                    sbatch_cmd, capture_output=True, text=True, check=True
                )

                if dry_run:
                    # For dry run, we don't get a real job ID
                    job_id = f"DRY_RUN_{job_name}"
                else:
                    # Parse job ID from sbatch output: "Submitted batch job 12345"
                    output_line = result.stdout.strip()
                    job_id = output_line.split()[-1]

                submitted_jobs[job_name] = job_id
                print(f"  Job ID: {job_id}")

            except subprocess.CalledProcessError as e:
                error_msg = f"Failed to submit {job_name}: {e.stderr}"
                print(f"  ERROR: {error_msg}")
                raise RuntimeError(error_msg)

        self.submitted_jobs.update(submitted_jobs)
        return submitted_jobs

    def _get_submission_order(self) -> List[str]:
        """Get job submission order respecting dependencies"""
        # Topological sort of jobs based on dependencies
        in_degree = {}

        # Initialize in-degree count
        for job_name in self.job_scripts.keys():
            in_degree[job_name] = 0

        # Count incoming edges (dependencies)
        for job_name, deps in self.job_dependencies.items():
            for dep in deps:
                if dep in in_degree:  # Only count dependencies that exist
                    in_degree[job_name] += 1

        # Queue of jobs with no dependencies
        queue = [job for job, degree in in_degree.items() if degree == 0]
        submission_order = []

        while queue:
            current_job = queue.pop(0)
            submission_order.append(current_job)

            # Reduce in-degree for dependent jobs
            for job_name, deps in self.job_dependencies.items():
                if current_job in deps:
                    in_degree[job_name] -= 1
                    if in_degree[job_name] == 0:
                        queue.append(job_name)

        return submission_order

    def get_job_status(self) -> Dict[str, str]:
        """
        Get status of submitted jobs using squeue

        Returns:
            Dictionary mapping job names to their SLURM status
        """
        if not self.submitted_jobs:
            return {}

        job_status = {}

        for job_name, job_id in self.submitted_jobs.items():
            if job_id.startswith("DRY_RUN"):
                job_status[job_name] = "DRY_RUN"
                continue

            try:
                # Query job status
                result = subprocess.run(
                    ["squeue", "-j", job_id, "-h", "-o", "%T"],
                    capture_output=True,
                    text=True,
                    check=True,
                )

                if result.stdout.strip():
                    job_status[job_name] = result.stdout.strip()
                else:
                    # Job not found in queue, might be completed
                    job_status[job_name] = "COMPLETED"

            except subprocess.CalledProcessError:
                job_status[job_name] = "UNKNOWN"

        return job_status


def test_pipeline_driver():
    """Test PipelineDriver functionality"""
    import tempfile
    from pathlib import Path

    print("=== Pipeline Driver Test ===\n")

    # Create test .def file
    def_content = """[common]
vis = pipeline_test.ms
telescope = EVLA
imsize = 512
cell = 12
stokes = I
reffreq = 1.4GHz
phasecenter = 19:59:58.500000 +40.40.00.00000 J2000
basename = pipeline_test
iterations = 2

[slurm]
account = test_account
email = test@msu.edu
coyote_nprocs = 8
default_walltime = 3:00:00
coyote_mem = 4GB

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
"""

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create .def file
        def_file = tmpdir / "test.def"
        with open(def_file, "w") as f:
            f.write(def_content)

        # Create config parser
        config = ConfigParser(str(def_file))

        # Initialize pipeline driver
        driver = PipelineDriver(config, str(tmpdir / "pipeline_output"))

        print(f"Pipeline working directory: {driver.working_dir}")

        # Mock application for testing
        class MockCoyoteJob:
            def __init__(self):
                pass

            def validate_requirements(self):
                pass  # Mock validation

            def generate_jobs(self):
                return [
                    {
                        "job_name": "pipeline_test_coyote_dryrun",
                        "type": "single",
                        "phase": "dryrun",
                        "script_path": str(tmpdir / "dryrun.sh"),
                        "depends_on": None,
                    },
                    {
                        "job_name": "pipeline_test_coyote_fillcf",
                        "type": "array",
                        "phase": "fillcf",
                        "script_path": str(tmpdir / "fillcf.sh"),
                        "depends_on_job": "pipeline_test_coyote_dryrun",
                    },
                ]

        # Create mock scripts
        for script_name in ["dryrun.sh", "fillcf.sh"]:
            script_path = tmpdir / script_name
            script_path.write_text('#!/bin/bash\necho "mock script"')
            script_path.chmod(0o755)

        # Add mock application
        mock_coyote = MockCoyoteJob()
        driver.add_application("coyote", mock_coyote)

        print(f"Added applications: {list(driver.applications.keys())}")

        # Generate scripts
        generated_jobs = driver.generate_all_scripts(dry_run=True)

        print(f"Generated jobs for {len(generated_jobs)} applications:")
        for app_name, jobs in generated_jobs.items():
            print(f"  {app_name}: {len(jobs)} jobs")

        # Print pipeline summary
        driver.print_pipeline_summary()

        # Test validation
        is_valid, errors = driver.validate_pipeline()
        print(f"\nPipeline validation: {'PASSED' if is_valid else 'FAILED'}")
        if errors:
            for error in errors:
                print(f"  Error: {error}")

        # Test submission order
        submission_order = driver._get_submission_order()
        print(f"\nSubmission order: {submission_order}")

        print("\nPipelineDriver test completed successfully! ✓")


if __name__ == "__main__":
    test_pipeline_driver()
