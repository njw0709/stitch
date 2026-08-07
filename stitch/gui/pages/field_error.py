"""
Shared error highlighting for the validating wizard pages.
"""


class FieldErrorMixin:
    """Red-border highlighting for widgets a page's ``validatePage`` rejected.

    Mixed into every wizard page that validates its inputs, so the highlight
    looks the same everywhere. List it before ``QWizardPage`` in the bases; it
    defines no ``__init__``, so Qt's construction is untouched.
    """

    ERROR_STYLE = "border: 2px solid #dc3545; border-radius: 3px;"

    def _set_field_error(self, widget, has_error: bool) -> None:
        """Toggle the error border on *widget*."""
        widget.setStyleSheet(self.ERROR_STYLE if has_error else "")
