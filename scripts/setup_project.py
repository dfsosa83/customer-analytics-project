#!/usr/bin/env python3
"""
Project setup script for Customer Analytics Project.

This script helps initialize the project environment and verify prerequisites.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


def check_python_version():
    """Check if Python version is 3.11 or higher."""
    if sys.version_info < (3, 11):
        print("❌ Python 3.11 or higher is required")
        print(f"Current version: {sys.version}")
        return False
    print(f"✅ Python version: {sys.version.split()[0]}")
    return True


def check_docker():
    """Check if Docker is installed and running."""
    try:
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, check=True)
        print(f"✅ Docker: {result.stdout.strip()}")
        
        # Check if Docker daemon is running
        subprocess.run(['docker', 'info'], 
                      capture_output=True, check=True)
        print("✅ Docker daemon is running")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Docker is not installed or not running")
        return False


def check_docker_compose():
    """Check if Docker Compose is available."""
    try:
        result = subprocess.run(['docker-compose', '--version'], 
                              capture_output=True, text=True, check=True)
        print(f"✅ Docker Compose: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        try:
            # Try docker compose (newer syntax)
            result = subprocess.run(['docker', 'compose', 'version'], 
                                  capture_output=True, text=True, check=True)
            print(f"✅ Docker Compose: {result.stdout.strip()}")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("❌ Docker Compose is not installed")
            return False


def create_directories():
    """Create necessary project directories."""
    directories = [
        'data/raw',
        'data/processed', 
        'data/exports',
        'logs',
        'tests/test_services',
        'tests/test_routers',
        'tests/test_utils'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")


def setup_environment():
    """Setup environment file if it doesn't exist."""
    env_file = Path('.env')
    env_template = Path('.env.template')
    
    if not env_file.exists() and env_template.exists():
        shutil.copy(env_template, env_file)
        print("✅ Created .env file from template")
        print("⚠️  Please edit .env file with your Snowflake credentials")
    elif env_file.exists():
        print("✅ .env file already exists")
    else:
        print("❌ .env.template not found")


def install_dependencies():
    """Install Python dependencies."""
    try:
        print("Installing Python dependencies...")
        subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], 
                      check=True)
        print("✅ Python dependencies installed")
        return True
    except subprocess.CalledProcessError:
        print("❌ Failed to install Python dependencies")
        return False


def main():
    """Main setup function."""
    print("🚀 Customer Analytics Project Setup")
    print("=" * 50)
    
    # Check prerequisites
    checks_passed = True
    checks_passed &= check_python_version()
    checks_passed &= check_docker()
    checks_passed &= check_docker_compose()
    
    if not checks_passed:
        print("\n❌ Some prerequisites are missing. Please install them and try again.")
        sys.exit(1)
    
    print("\n📁 Setting up project structure...")
    create_directories()
    setup_environment()
    
    # Ask user if they want to install dependencies
    install_deps = input("\n📦 Install Python dependencies? (y/N): ").lower().strip()
    if install_deps in ['y', 'yes']:
        install_dependencies()
    
    print("\n🎉 Project setup complete!")
    print("\nNext steps:")
    print("1. Edit .env file with your Snowflake credentials")
    print("2. Run: docker-compose up --build -d")
    print("3. Visit: http://localhost:8000/docs")
    print("\nFor more information, see README.md and QUICKSTART.md")


if __name__ == "__main__":
    main()
