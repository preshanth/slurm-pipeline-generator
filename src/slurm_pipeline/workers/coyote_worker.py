#!/usr/bin/env python3
"""
Coyote worker module for SLURM pipeline generation

This module handles the execution of coyote commands in both dryrun and fillcf modes.
It's designed to be called as a standalone script from SLURM job scripts.

Usage:
    python coyote_worker.py --mode dryrun --cfcache_dir /path/to/cache --nprocs 40 --coyote_app /path/to/coyote
    python coyote_worker.py --mode fillcf --cfcache_dir /path/to/cache --nprocs 40 --coyote_app /path/to/coyote
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional


class CoyoteWorker:
    """Worker class for executing coyote commands"""
    
    def __init__(self, 
                 cfcache_dir: str,
                 nprocs: int,
                 coyote_app: str,
                 common_params: Optional[Dict[str, Any]] = None,
                 app_params: Optional[Dict[str, Any]] = None):
        """
        Initialize coyote worker
        
        Args:
            cfcache_dir: Directory for CF cache
            nprocs: Number of processes
            coyote_app: Path to coyote executable
            common_params: Common parameters from config
            app_params: Application-specific parameters from config
        """
        self.cfcache_dir = Path(cfcache_dir)
        self.nprocs = nprocs
        self.coyote_app = Path(coyote_app)
        self.common_params = common_params or {}
        self.app_params = app_params or {}
        
        # Validate inputs
        self.validate_setup()
    
    def validate_setup(self) -> None:
        """Validate worker setup"""
        if not self.coyote_app.exists():
            raise FileNotFoundError(f"Coyote executable not found: {self.coyote_app}")
        
        if not self.coyote_app.is_file():
            raise ValueError(f"Coyote path is not a file: {self.coyote_app}")
        
        if self.nprocs <= 0:
            raise ValueError(f"Invalid number of processes: {self.nprocs}")
        
        # Validate data directory setup
        self.validate_data_environment()
    
    def validate_data_environment(self) -> None:
        """Validate data directory and VLA surface file"""
        # Check if data directory exists in current working directory
        data_dir = Path.cwd() / "data"
        if not data_dir.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")
        
        # Check for VLA surface file
        vla_surface = data_dir / "nrao" / "VLA" / "VLA.surface"
        if not vla_surface.exists():
            raise FileNotFoundError(f"VLA surface file not found: {vla_surface}")
        
        print(f"Data environment validated: {data_dir}")
        print(f"VLA surface file found: {vla_surface}")
    
    def setup_environment(self) -> None:
        """Set up environment variables for coyote execution"""
        # Set CASAPATH to point to our data directory
        data_dir = Path.cwd() / "data"
        casapath = str(data_dir.absolute())
        
        os.environ["CASAPATH"] = casapath
        print(f"CASAPATH set to: {casapath}")
        
        # Verify the environment is set correctly
        if "CASAPATH" not in os.environ:
            raise RuntimeError("Failed to set CASAPATH environment variable")
    
    def build_coyote_command(self, mode: str, process_id: Optional[int] = None) -> List[str]:
        """
        Build coyote command for the specified mode
        
        Args:
            mode: Either 'dryrun' or 'fillcf'
            process_id: Process ID for fillcf mode (from SLURM_ARRAY_TASK_ID)
        
        Returns:
            Command as list of strings
        """
        cmd = [str(self.coyote_app), "help=noprompt"]
        
        # Add common parameters
        for key, value in self.common_params.items():
            if key not in ["basename", "iterations"]:  # Skip non-coyote params
                cmd.append(f"{key}={value}")
        
        # Add app-specific parameters
        for key, value in self.app_params.items():
            cmd.append(f"{key}={value}")
        
        # Add cfcache parameter
        cmd.append(f"cfcache={self.cfcache_dir}")
        
        # Add mode-specific parameters
        if mode == "dryrun":
            # Dryrun mode creates empty CF cache structure
            pass  # cfcache parameter is sufficient
        elif mode == "fillcf":
            # Fillcf mode fills specific CF entries
            if process_id is None:
                raise ValueError("fillcf mode requires process_id")
            
            # Add fillcf-specific parameters
            cmd.extend([
                f"nprocs={self.nprocs}",
                f"procid={process_id}"
            ])
        else:
            raise ValueError(f"Unknown mode: {mode}")
        
        return cmd
    
    def run_dryrun(self) -> int:
        """
        Execute coyote in dryrun mode
        
        Returns:
            Exit code
        """
        print(f"Running coyote dryrun mode")
        print(f"CF cache directory: {self.cfcache_dir}")
        print(f"Number of processes: {self.nprocs}")
        
        # Set up environment variables
        self.setup_environment()
        
        # Ensure cfcache directory exists
        self.cfcache_dir.mkdir(parents=True, exist_ok=True)
        
        # Build and execute command
        cmd = self.build_coyote_command("dryrun")
        
        print(f"Executing: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=False)
            print("Dryrun completed successfully")
            return result.returncode
        except subprocess.CalledProcessError as e:
            print(f"Dryrun failed with exit code {e.returncode}")
            return e.returncode
        except Exception as e:
            print(f"Dryrun failed with error: {e}")
            return 1
    
    def run_fillcf(self) -> int:
        """
        Execute coyote in fillcf mode
        
        Returns:
            Exit code
        """
        # Get process ID from SLURM array task ID
        process_id = os.environ.get('SLURM_ARRAY_TASK_ID')
        if process_id is None:
            print("ERROR: SLURM_ARRAY_TASK_ID not found. This should be run as an array job.")
            return 1
        
        try:
            process_id = int(process_id)
        except ValueError:
            print(f"ERROR: Invalid SLURM_ARRAY_TASK_ID: {process_id}")
            return 1
        
        print(f"Running coyote fillcf mode for process {process_id}/{self.nprocs-1}")
        print(f"CF cache directory: {self.cfcache_dir}")
        
        # Set up environment variables
        self.setup_environment()
        
        # Verify cfcache directory exists
        if not self.cfcache_dir.exists():
            print(f"ERROR: CF cache directory does not exist: {self.cfcache_dir}")
            return 1
        
        # Build and execute command
        cmd = self.build_coyote_command("fillcf", process_id)
        
        print(f"Executing: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd, check=True, capture_output=False)
            print(f"Fillcf process {process_id} completed successfully")
            return result.returncode
        except subprocess.CalledProcessError as e:
            print(f"Fillcf process {process_id} failed with exit code {e.returncode}")
            return e.returncode
        except Exception as e:
            print(f"Fillcf process {process_id} failed with error: {e}")
            return 1


def load_json_params(file_path: str) -> Dict[str, Any]:
    """Load parameters from JSON file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Warning: Could not load parameters from {file_path}: {e}")
        return {}


def main() -> int:
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Coyote worker for SLURM pipeline")
    
    parser.add_argument("--mode", required=True, choices=["dryrun", "fillcf"],
                       help="Execution mode")
    parser.add_argument("--cfcache_dir", required=True,
                       help="CF cache directory")
    parser.add_argument("--nprocs", type=int, required=True,
                       help="Number of processes")
    parser.add_argument("--coyote_app", required=True,
                       help="Path to coyote executable")
    parser.add_argument("--common_params_file", 
                       help="Path to common parameters JSON file")
    parser.add_argument("--app_params_file",
                       help="Path to app parameters JSON file")
    
    args = parser.parse_args()
    
    # Load parameter files
    common_params = {}
    app_params = {}
    
    if args.common_params_file:
        common_params = load_json_params(args.common_params_file)
    
    if args.app_params_file:
        app_params = load_json_params(args.app_params_file)
    
    # Create worker and execute
    try:
        worker = CoyoteWorker(
            cfcache_dir=args.cfcache_dir,
            nprocs=args.nprocs,
            coyote_app=args.coyote_app,
            common_params=common_params,
            app_params=app_params
        )
        
        if args.mode == "dryrun":
            return worker.run_dryrun()
        elif args.mode == "fillcf":
            return worker.run_fillcf()
        else:
            print(f"ERROR: Unknown mode: {args.mode}")
            return 1
            
    except Exception as e:
        print(f"ERROR: Worker failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())