"""Custom exceptions for the data interface layer."""


class DataLayerError(Exception):
    """Base exception for the data layer."""


class DataSourceUnavailable(DataLayerError):
    """Raised when a remote or local data source cannot serve a request."""


class TushareAPIError(DataLayerError):
    """Raised when Tushare returns a non-zero business error code."""


class SchemaValidationError(DataLayerError):
    """Raised when a frame cannot be normalized into the expected schema."""
