"""
Linkdata package for linking survey data with contextual daily measures.
"""

try:
    from importlib.metadata import version, PackageNotFoundError

    try:
        __version__ = version("linkage")
    except PackageNotFoundError:
        __version__ = "0.1.0"  # Fallback version
except ImportError:
    __version__ = "0.1.0"  # Fallback for Python < 3.8

from .hrs import ResidentialHistoryHRS, HRSInterviewData, HRSContextLinker
from .daily_measure import (
    DailyMeasureData,
    DailyMeasureDataDir,
    aggregate_contextual_to_resolution,
)
from .io_utils import read_data, write_data, get_file_format
from .temporal import LinkageResolution, AggMethod, infer_temporal_resolution
from .process import (
    compute_required_years,
    extract_unique_geoids,
    process_multiple_lags_batch,
    process_multiple_lags_parallel,
)
from .validation import (
    check_contextual_column_roles,
    check_generated_column_collisions,
    check_output_path_conflicts,
    check_pipeline_args,
    check_pipeline_survey_columns,
    check_residential_history_column_roles,
    check_survey_column_roles,
    validate_pipeline_args,
    validate_pipeline_survey_columns,
)

__all__ = [
    # Version
    "__version__",
    # HRS classes
    "ResidentialHistoryHRS",
    "HRSInterviewData",
    "HRSContextLinker",
    # Daily measure classes
    "DailyMeasureData",
    "DailyMeasureDataDir",
    "aggregate_contextual_to_resolution",
    # I/O utilities
    "read_data",
    "write_data",
    "get_file_format",
    # Temporal resolution
    "LinkageResolution",
    "AggMethod",
    "infer_temporal_resolution",
    # Processing functions
    "compute_required_years",
    "extract_unique_geoids",
    "process_multiple_lags_batch",
    "process_multiple_lags_parallel",
    # Configuration validation
    "check_survey_column_roles",
    "check_residential_history_column_roles",
    "check_contextual_column_roles",
    "check_output_path_conflicts",
    "check_generated_column_collisions",
    "check_pipeline_args",
    "check_pipeline_survey_columns",
    "validate_pipeline_args",
    "validate_pipeline_survey_columns",
]
