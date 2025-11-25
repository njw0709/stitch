"""
PyInstaller runtime hook for PyQt6.

This hook handles platform-specific Qt initialization issues:
- macOS: Disables location services plugin to prevent crashes
- Windows: Enables DPI awareness for high-resolution displays
- All platforms: Configures error logging to user's home directory
"""

import os
import sys
from pathlib import Path

# Configure error logging for all platforms
log_file = Path.home() / ".stitch.log"
os.environ["HRS_LINKAGE_LOG_FILE"] = str(log_file)

# Platform-specific initialization
if sys.platform == "darwin":
    # Disable Qt permission plugins that cause crashes
    os.environ["QT_MAC_WANTS_LAYER"] = "1"

    # Prevent Qt from trying to access location services
    # This prevents the crash in warmUpLocationServices()
    os.environ["QT_LOGGING_RULES"] = "qt.permissions*=false;qt.qpa.plugin=false"

    # Explicitly disable permission-related Qt functionality
    os.environ["QT_QPA_PLATFORMTHEME"] = ""

elif sys.platform == "win32":
    # Enable DPI awareness on Windows for high-resolution displays
    try:
        from ctypes import windll

        # System DPI aware (1) - app scales with system DPI
        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        # Ignore errors on older Windows versions without shcore.dll
        pass
