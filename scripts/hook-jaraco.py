"""
PyInstaller hook for jaraco namespace package.

The jaraco package is a namespace package used by many dependencies
(like importlib-metadata, setuptools, etc). This hook ensures all
jaraco submodules are properly collected.
"""

from PyInstaller.utils.hooks import collect_submodules, collect_data_files

# Collect all jaraco submodules
hiddenimports = collect_submodules("jaraco")

# Collect any data files from jaraco packages
datas = collect_data_files("jaraco")
