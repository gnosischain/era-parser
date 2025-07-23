"""
Era Parser - Ethereum Beacon Chain Era File Parser

A modular parser for Ethereum beacon chain era files supporting multiple
networks, forks, and export formats.
"""

__version__ = "1.0.0"
__author__ = "Era Parser Team"

from .ingestion.era_reader import EraReader
from .parsing.block_parser import BlockParser
from .export.json_exporter import JSONExporter
from .export.csv_exporter import CSVExporter
from .export.parquet_exporter import ParquetExporter

__all__ = [
    "EraReader",
    "BlockParser", 
    "JSONExporter",
    "CSVExporter",
    "ParquetExporter"
]