# src/synthetic_data_pipeline/__init__.py

"""Synthetic Data Pipeline - AI-powered data generation."""

__version__ = "0.1.4"

from .main import run_pipeline, generate_synthetic_data
from .utils.pipeline_state_manager import PipelineStateManager

__all__ = [
    "run_pipeline",
    "generate_synthetic_data",
    "PipelineStateManager",
    "__version__"
]