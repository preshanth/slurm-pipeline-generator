# SLURM Pipeline Generator

A Python package for generating SLURM job scripts for radio astronomy data processing pipelines.

## Installation

```bash
# Development installation
pip install -e .[dev]
```

## Quick Start

```python
from slurm_pipeline import ConfigParser, PipelineDriver
from slurm_pipeline.applications import CoyoteJob

# Parse configuration
config = ConfigParser("pipeline.def")

# Create and configure jobs
coyote_job = CoyoteJob(config, coyote_binary="/path/to/coyote")

# Generate scripts
jobs = coyote_job.generate_jobs()
print(f"Generated {len(jobs)} job scripts")
```

## Development

```bash
# Run tests
pytest

# Run tests with coverage
pytest --cov=src/slurm_pipeline --cov-report=html

# Format code
black src/ tests/

# Type checking
mypy src/
```

## Architecture

- **Core Components**: Composition-based design with CommandBuilder, ResourceConfig, FileManager, ScriptGenerator
- **Job Types**: SingleJob, ArrayJob, GPUJob with clean inheritance
- **Applications**: CoyoteJob (implemented), RoadrunnerJob (future), DaleJob (future), HummbeeJob (future)

## Configuration

See `examples/sample.def` for configuration file format.
