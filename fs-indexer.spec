# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['fs_indexer/main.py'],
    pathex=['/Users/davidphillips/Documents/1_Projects/LucidLink/fs-indexer-lucidlink'],
    binaries=[],
    datas=[
        ('fs_indexer/indexer-config.yaml', 'fs_indexer'),
        ('fs_indexer/schema.py', 'fs_indexer'),
        ('fs_indexer/db_optimizations.py', 'fs_indexer'),
    ],
    hiddenimports=[
        'duckdb',
        'aiohttp',
        'yaml',
        'fs_indexer.schema',
        'fs_indexer.db_optimizations',
    ],
    hookspath=[],
    hooksconfig={},
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
