#!/usr/bin/env python
"""
GUI application entry point for the STITCH.

This script launches a PyQt6-based wizard interface that guides users through
configuring and executing the lagged contextual data linkage pipeline.

Usage:
    python gui_app.py
    # or
    uv run python gui_app.py
"""
import sys
import os
import traceback
from pathlib import Path
from datetime import datetime

from PyQt6.QtWidgets import QApplication

from stitch.gui.main_window import LinkageWizard


def get_log_file():
    """Get the error log file path from environment or use default."""
    log_path = os.environ.get("STITCH_LOG_FILE")
    if log_path:
        return Path(log_path)
    return Path.home() / ".stitch.log"


def log_error(error_msg, exception=None):
    """Write error to log file for debugging startup issues."""
    try:
        log_file = get_log_file()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"\n{'='*70}\n")
            f.write(f"[{timestamp}] STITCH Error\n")
            f.write(f"{'='*70}\n")
            f.write(f"{error_msg}\n")

            if exception:
                f.write("\nTraceback:\n")
                f.write(traceback.format_exc())

            f.write(f"{'='*70}\n")
    except Exception:
        # If we can't write to log, silently fail (app is already broken)
        pass


def main():
    """Launch the STITCH GUI."""
    try:
        app = QApplication(sys.argv)

        # Set application metadata
        app.setApplicationName("STITCH")
        app.setOrganizationName("CBPH Research")

        # Create and show wizard
        wizard = LinkageWizard()
        wizard.show()

        # Run application
        sys.exit(app.exec())

    except Exception as e:
        error_msg = f"Failed to start STITCH GUI: {str(e)}"
        log_error(error_msg, exception=e)

        # Try to show error dialog if Qt is available
        try:
            from PyQt6.QtWidgets import QMessageBox

            msg = QMessageBox()
            msg.setIcon(QMessageBox.Icon.Critical)
            msg.setWindowTitle("Startup Error")
            msg.setText("STITCH failed to start")
            msg.setInformativeText(
                f"{error_msg}\n\nSee log file for details:\n{get_log_file()}"
            )
            msg.exec()
        except Exception:
            # If Qt dialog fails, write to stderr
            print(f"ERROR: {error_msg}", file=sys.stderr)
            print(f"See log file: {get_log_file()}", file=sys.stderr)

        sys.exit(1)


if __name__ == "__main__":
    main()
