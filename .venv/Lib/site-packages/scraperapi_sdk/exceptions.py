class ScraperAPIException(Exception):
    """Base class for exceptions in this module."""

    def __init__(self, message, original_exception):
        super().__init__(message)
        self.original_exception = original_exception
