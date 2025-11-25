# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller specification file for HRS Linkage Tool GUI.

This spec file configures the build of a standalone executable for the
PyQt6-based GUI application.
"""

import sys
import os
import shutil
from PyInstaller.utils.hooks import collect_submodules, copy_metadata
from PyQt6 import QtCore

block_cipher = None

# Collect all submodules from critical packages
stitch_submodules = collect_submodules('stitch')

# Additional hidden imports - let PyInstaller's hooks handle pandas/numpy automatically
hidden_imports = [
    # Core data processing
    'pandas',
    'numpy',
    'openpyxl',
    'pyarrow',
    # Utilities
    'psutil',
    'tqdm',
    # PyQt6
    'PyQt6.QtCore',
    'PyQt6.QtGui',
    'PyQt6.QtWidgets',
    # importlib.metadata dependencies (required for version checking)
    'importlib.metadata',
    'importlib_metadata',
    'jaraco',
    'jaraco.text',
    'jaraco.functools',
    'jaraco.context',
] + stitch_submodules

# Collect Qt plugins - required for all platforms
# These plugins are essential for Qt to function properly
qt_plugins_path = os.path.join(os.path.dirname(QtCore.__file__), 'Qt6', 'plugins')

# AGGRESSIVELY remove problematic Qt plugins on macOS BEFORE bundling
if sys.platform == 'darwin':
    problematic_plugins = [
        os.path.join(qt_plugins_path, 'permissions'),
        os.path.join(qt_plugins_path, 'position'),
    ]
    for plugin_dir in problematic_plugins:
        if os.path.exists(plugin_dir):
            print(f"Removing problematic Qt plugin directory: {plugin_dir}")
            shutil.rmtree(plugin_dir, ignore_errors=True)

datas = [
    (os.path.join(qt_plugins_path, 'platforms'), 'PyQt6/Qt6/plugins/platforms'),
    (os.path.join(qt_plugins_path, 'styles'), 'PyQt6/Qt6/plugins/styles'),
]

# Collect metadata for packages used by importlib.metadata
# This is required for version checking and other metadata operations
try:
    datas += copy_metadata('importlib_metadata')
except Exception:
    pass  # Package might not be installed

try:
    datas += copy_metadata('stitch')
except Exception:
    pass  # Package might not be installed

# Function to filter out problematic Qt plugins on macOS
def filter_binaries(binaries):
    """Remove problematic Qt plugins that cause crashes on macOS."""
    if sys.platform != 'darwin':
        return binaries
    
    excluded_patterns = [
        'libqdarwinpermission',  # Permission plugins (location, camera, etc.)
        'qdarwinpermission',
        'permissions',
        'QtLocation',
        'QtPositioning',
        'QtBluetooth',
        'QtNfc',
    ]
    
    filtered = []
    for dest, source, kind in binaries:
        exclude = False
        for pattern in excluded_patterns:
            # Case-insensitive matching for better compatibility
            if pattern.lower() in source.lower() or pattern.lower() in dest.lower():
                print(f"Excluding binary: {dest} ({source})")
                exclude = True
                break
        if not exclude:
            filtered.append((dest, source, kind))
    
    return filtered

a = Analysis(
    ['gui_app.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=hidden_imports,
    hookspath=['scripts'],
    hooksconfig={},
    runtime_hooks=['scripts/hook-PyQt6.py'],
    excludes=[
        'matplotlib',
        'scipy',
        'tkinter',
        'pytest',  # Exclude pytest to avoid the f2py.tests warning
        # Exclude Qt plugins that cause crashes on macOS
        'PyQt6.QtPositioning',
        'PyQt6.QtLocation',
        'PyQt6.QtBluetooth',
        'PyQt6.QtNfc',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# Filter out problematic binaries on macOS
a.binaries = filter_binaries(a.binaries)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='STITCH',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,  # Disabled: UPX can corrupt Qt DLLs and trigger antivirus false positives
    console=False,  # Set to False for GUI app (no console window)
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='icons/icon.ico',
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,  # Disabled: UPX can corrupt Qt DLLs and trigger antivirus false positives
    upx_exclude=[],
    name='STITCH',
)

# macOS app bundle
if sys.platform == 'darwin':
    app = BUNDLE(
        coll,
        name='STITCH.app',
        icon="icons/Icon.icns",
        bundle_identifier='org.stitch.tool',
        info_plist={
            'CFBundleName': 'STITCH',
            'CFBundleDisplayName': 'STITCH',
            'CFBundleVersion': '0.1.0',
            'CFBundleShortVersionString': '0.1.0',
            'NSHighResolutionCapable': 'True',
            # Privacy keys to prevent Qt from accessing unnecessary services
            'NSLocationWhenInUseUsageDescription': 'This app does not use location services.',
            'NSLocationAlwaysAndWhenInUseUsageDescription': 'This app does not use location services.',
            'NSCameraUsageDescription': 'This app does not use the camera.',
            'NSMicrophoneUsageDescription': 'This app does not use the microphone.',
        },
    )

