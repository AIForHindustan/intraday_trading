"""
Binary to Parquet Converter Package
=================================

This package provides tools for converting Zerodha binary tick data to Parquet format
and organizing the data by date for efficient querying and analysis.

Main Components:
- production_binary_converter: Production converter class for binary to Parquet conversion
- Date-organized parquet files: Organized by YYYY-MM-DD directories

Usage:
    from binary_to_parquet.production_binary_converter import ProductionZerodhaBinaryConverter
    from token_cache import TokenCacheManager
    
    # Convert binary files
    token_cache = TokenCacheManager()
    converter = ProductionZerodhaBinaryConverter(token_cache=token_cache)
    batches = converter.convert_file_to_arrow_batches(file_path)
"""

from .production_binary_converter import ProductionZerodhaBinaryConverter

__all__ = ['ProductionZerodhaBinaryConverter']
