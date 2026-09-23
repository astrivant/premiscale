"""
Custom exceptions for the premiscale package.
"""


from http import HTTPStatus


class RateLimitedError(Exception):
    """
    Raised when an HTTPStatus.TOO_MANY_REQUESTS code is received.
    """
    def __init__(self, message: str ='', delay: float =30.0) -> None:
        """
        Initialize RateLimitedError with the supplied settings.

        Args:
            message (str): Human-readable exception description.
            delay (float): Delay before retrying a rate-limited request, in seconds.
        """
        super().__init__(message)
        self.message = message
        self.delay = delay

    def __str__(self) -> str:
        """
        Return a readable representation of this value.

        Returns:
            str: A readable representation of this value.
        """
        return f'RateLimitedError(message="{self.message}", code="{HTTPStatus.TOO_MANY_REQUESTS}", "x-rate-limit-reset={self.delay}")'
