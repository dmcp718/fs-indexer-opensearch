
# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['fs_indexer/main.py'],
    pathex=['/Users/davidphillips/Documents/1_Projects/LucidLink/FS_OpenSearch_index_search/fs-indexer-migrator-dbos/fs-indexer'],
    binaries=[],
    datas=[
        ('fs_indexer/indexer-config.yaml', 'fs_indexer'),
        ('fs_indexer/schema.py', 'fs_indexer'),
        ('fs_indexer/db_optimizations.py', 'fs_indexer'),
    ],
    hiddenimports=[
        'sqlalchemy',
        'sqlalchemy.sql.default_comparator',
        'yaml',
        'redis',
        'fs_indexer.schema',
        'fs_indexer.db_optimizations',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
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
