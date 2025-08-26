
# tests/unit/test_base_jobs.py
"""
Unit tests for base job classes
"""

import pytest
import tempfile
from pathlib import Path

# Mock config parser for testing
class MockConfigParser:
    def get_common_params(self):
        return {
            'vis': 'test.ms',
            'basename': 'test_run',
            'telescope': 'EVLA'
        }
    
    def get_slurm_config(self):
        return {
            'account': 'test_account',
            'email': 'test@msu.edu',
            'gpu_type': 'h200',
            'default_walltime': '2:00:00',
            'coyote_mem': '4GB',
            'roadrunner_mem': '32GB'
        }
    
    def get_app_params(self, app_name):
        return {
            'mode': 'test',
            'param1': 'value1'
        }


# Test concrete implementations
class TestSingleJob:
    """Test SingleJob implementation"""
    
    def test_single_job_creation(self, temp_dir):
        """Test creating a single job"""
        from slurm_pipeline.core import SingleJob
        
        class ConcreteSingleJob(SingleJob):
            def get_app_name(self):
                return 'test_app'
            
            def setup_command_builder(self):
                self.command_builder.set_executable('/usr/bin/test')
                self.command_builder.add_base_args(help='noprompt')
            
            def generate_jobs(self):
                return [self.generate_single_job('test_single', 'coyote_mem')]
        
        config = MockConfigParser()
        single_job = ConcreteSingleJob(config, str(temp_dir))
        jobs = single_job.generate_jobs()
        
        assert len(jobs) == 1
        assert jobs[0]['type'] == 'single'
        assert jobs[0]['job_name'] == 'test_single'
        assert Path(jobs[0]['script_path']).exists()


class TestArrayJob:
    """Test ArrayJob implementation"""
    
    def test_array_job_creation(self, temp_dir):
        """Test creating an array job"""
        from slurm_pipeline.core import ArrayJob
        
        class ConcreteArrayJob(ArrayJob):
            def get_app_name(self):
                return 'test_app'
            
            def setup_command_builder(self):
                self.command_builder.set_executable('/usr/bin/test')
                self.command_builder.add_base_args(help='noprompt')
            
            def generate_jobs(self):
                return [self.generate_array_job('test_array', 'coyote_mem', '0-7')]
        
        config = MockConfigParser()
        array_job = ConcreteArrayJob(config, str(temp_dir))
        jobs = array_job.generate_jobs()
        
        assert len(jobs) == 1
        assert jobs[0]['type'] == 'array'
        assert jobs[0]['array_range'] == '0-7'
        assert Path(jobs[0]['script_path']).exists()


class TestGPUJob:
    """Test GPUJob implementation"""
    
    def test_gpu_job_creation(self, temp_dir):
        """Test creating a GPU job"""
        from slurm_pipeline.core import GPUJob
        
        class ConcreteGPUJob(GPUJob):
            def get_app_name(self):
                return 'test_app'
            
            def setup_command_builder(self):
                self.command_builder.set_executable('/usr/bin/test')
                self.command_builder.add_base_args(help='noprompt')
            
            def generate_jobs(self):
                return [self.generate_gpu_job('test_gpu', 'roadrunner_mem')]
        
        config = MockConfigParser()
        gpu_job = ConcreteGPUJob(config, str(temp_dir))
        jobs = gpu_job.generate_jobs()
        
        assert len(jobs) == 1
        assert jobs[0]['type'] == 'gpu'
        assert jobs[0]['gpu_type'] == 'h200'
        assert jobs[0]['gpu_count'] == 1