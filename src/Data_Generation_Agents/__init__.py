# src/synthetic_data_pipeline/__init__.py

"""Data_Generation_Agents - An AI-powered synthetic data generation pipeline with persistent state management."""

__version__ = "0.1.6"

from .main import run_pipeline, generate_synthetic_data

__all__ = [
    "run_pipeline",
    "generate_synthetic_data",
    "__version__"
]