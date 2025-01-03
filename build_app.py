#!/usr/bin/env python3

import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

def run_command(cmd):
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

def build_app():
    # Determine the operating system
    system = platform.system().lower()
    if system not in ('darwin', 'linux'):
        print(f"Unsupported operating system: {system}")
        sys.exit(1)

    # Setup paths
    root_dir = Path(__file__).parent.absolute()
    dist_dir = root_dir / 'dist'
    build_dir = root_dir / 'build'
    config_file = root_dir / 'fs_indexer' / 'indexer-config.yaml'
    pyinstaller_path = root_dir / 'venv' / 'bin' / 'pyinstaller'
    platform_dir = dist_dir / f'fs-indexer-{system}'

    # Clean previous builds
    for dir_path in (dist_dir, build_dir):
        if dir_path.exists():
            shutil.rmtree(dir_path)

    # Create spec file content
    spec_content = f'''
# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['fs_indexer/main.py'],
    pathex=['{root_dir}'],
    binaries=[],
    datas=[
        ('fs_indexer/indexer-config.yaml', '.'),
        ('fs_indexer/schema.py', '.'),
        ('fs_indexer/db_optimizations.py', '.'),
    ],
    hiddenimports=[
        'duckdb',
        'aiohttp',
        'yaml',
        'fs_indexer.schema',
        'fs_indexer.db_optimizations',
    ],
    hookspath=[],
    hooksconfig={{}},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=None)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='fs-indexer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
'''

    # Write spec file
    spec_file = root_dir / 'fs-indexer.spec'
    spec_file.write_text(spec_content)

    # Run PyInstaller
    pyinstaller_cmd = [
        str(pyinstaller_path),
        '--clean',
        f'--distpath={platform_dir}',
        str(spec_file)
    ]
    print(f"Running: {' '.join(pyinstaller_cmd)}")
    subprocess.run(pyinstaller_cmd, check=True)

    # Copy config file to output directory
    shutil.copy2(config_file, platform_dir / 'indexer-config.yaml')

    # Create run script
    run_script = platform_dir / 'run-indexer.sh'
    run_script.write_text('#!/bin/bash\n./fs-indexer "$@"\n')
    run_script.chmod(0o755)

    print(f"\nBuild complete! Release package created at: {platform_dir}")
    print("\nTo run the indexer:")
    print(f"1. cd {platform_dir}")
    print("2. ./run-indexer.sh")

if __name__ == '__main__':
    build_app()
