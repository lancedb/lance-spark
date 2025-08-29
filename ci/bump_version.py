#!/usr/bin/env python3
"""
Version management script for Lance Spark project.
Uses bump-my-version to handle version bumping across all project components.
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list[str], capture_output: bool = True) -> subprocess.CompletedProcess:
    """Run a command and return the result."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=capture_output, text=True)
    if result.returncode != 0:
        print(f"Error running command: {' '.join(cmd)}")
        if capture_output:
            print(f"stderr: {result.stderr}")
        sys.exit(result.returncode)
    return result


def get_current_version() -> str:
    """Get the current version from .bumpversion.toml."""
    config_path = Path(".bumpversion.toml")
    if not config_path.exists():
        raise FileNotFoundError(".bumpversion.toml not found in current directory")
    
    with open(config_path, "r") as f:
        for line in f:
            if line.strip().startswith('current_version = "'):
                return line.split('"')[1]
    raise ValueError("Could not find current_version in .bumpversion.toml")


def main():
    parser = argparse.ArgumentParser(description='Bump version in Maven project using bump-my-version')
    parser.add_argument('--version', required=True, help='New version to set')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without making changes')
    
    args = parser.parse_args()
    
    # Get current version
    current_version = get_current_version()
    new_version = args.version
    
    print(f"Current version: {current_version}")
    print(f"New version: {new_version}")
    
    if args.dry_run:
        print("\nDry run mode - no changes will be made")
        # Run bump-my-version in dry-run mode
        cmd = ["bump-my-version", "bump", "--current-version", current_version, 
               "--new-version", new_version, "--dry-run", "--verbose", "--allow-dirty"]
        run_command(cmd, capture_output=False)
    else:
        # Use bump-my-version to update all files
        print("\nUpdating version in all files...")
        cmd = ["bump-my-version", "bump", "--current-version", current_version, 
               "--new-version", new_version, "--no-commit", "--no-tag", "--allow-dirty"]
        run_command(cmd)
        print(f"\nSuccessfully updated version from {current_version} to {new_version}")


if __name__ == '__main__':
    main()