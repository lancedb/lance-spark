#!/usr/bin/env python3
"""
Script to bump version in Maven project files
"""

import argparse
import xml.etree.ElementTree as ET
import os
import sys
from pathlib import Path

def update_pom_version(pom_path, new_version):
    """Update version in a pom.xml file"""
    # Register namespace
    ET.register_namespace('', 'http://maven.apache.org/POM/4.0.0')
    
    tree = ET.parse(pom_path)
    root = tree.getroot()
    
    # Define namespace
    ns = {'maven': 'http://maven.apache.org/POM/4.0.0'}
    
    # Update project version
    version_elem = root.find('maven:version', ns)
    if version_elem is not None:
        version_elem.text = new_version
    
    # Update lance-spark.version property if it exists
    properties = root.find('maven:properties', ns)
    if properties is not None:
        lance_spark_version = properties.find('maven:lance-spark.version', ns)
        if lance_spark_version is not None:
            lance_spark_version.text = new_version
    
    # Write back to file
    tree.write(pom_path, encoding='UTF-8', xml_declaration=True)
    print(f"Updated {pom_path}")

def find_pom_files(root_dir):
    """Find all pom.xml files in the project"""
    pom_files = []
    for path in Path(root_dir).rglob('pom.xml'):
        # Skip target directories and other build artifacts
        if 'target' not in str(path) and 'venv' not in str(path):
            pom_files.append(path)
    return sorted(pom_files)

def main():
    parser = argparse.ArgumentParser(description='Bump version in Maven project')
    parser.add_argument('--version', required=True, help='New version to set')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without making changes')
    
    args = parser.parse_args()
    
    # Find project root (where this script is located)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    # Find all pom.xml files
    pom_files = find_pom_files(project_root)
    
    if not pom_files:
        print("Error: No pom.xml files found", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(pom_files)} pom.xml files")
    print(f"New version: {args.version}")
    
    if args.dry_run:
        print("\nDry run mode - no changes will be made")
        for pom_file in pom_files:
            print(f"Would update: {pom_file}")
    else:
        for pom_file in pom_files:
            update_pom_version(pom_file, args.version)
        print(f"\nSuccessfully updated version to {args.version}")

if __name__ == '__main__':
    main()