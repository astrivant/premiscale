"""
Utility methods for the platform module.
"""

from __future__ import annotations

import time
import logging

from typing import TYPE_CHECKING
from functools import wraps
from urllib.error import URLError
from premiscale.errors import RateLimitedError

if TYPE_CHECKING:
    from typing import Callable, Any, Dict


log = logging.getLogger(__name__)


def retry(retries: int =0, retry_delay: float = 1.0, ratelimit_buffer: float = 0.25) -> Callable:
    """
    A request retry decorator that catches common request exceptions and retries the wrapped function call.

    Args:
        retries (int): number of times to retry the wrapped function call. When `0`, retries indefinitely. (default: 0)
        retry_delay (float): if retries is 0, this delay value (in seconds) is used between retries. (default: 1.0)
        ratelimit_buffer (float): a buffer (in seconds) to add to the delay when a rate limit is hit. (default: 0.25)

    Returns:
        Callable: a decorator for the wrapped function.

    Raises:
        ValueError: if `retries` is less than 0.
    """
    if retries < 0:
        raise ValueError(f'Expected positive `retries` values, received: "{retries}"')

    def decorator(f: Callable) -> Callable:
        """
        Wrap the supplied operation with retry handling.

        Args:
            f (Callable): Request callable to retry.

        Returns:
            Callable: Request wrapper applying the configured retry and rate-limit policy.
        """
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Dict | None:
            """
            Run the wrapped operation with the configured retry policy.

            Args:
                *args (Any): Positional arguments forwarded to the wrapped callable.
                **kwargs (Any): Keyword arguments forwarded to the wrapped callable.

            Returns:
                Dict | None: First successful response, or None when the finite retry limit is exhausted.
            """
            res: Any = None

            def call() -> Dict[str, str] | None:
                """
                Invoke the wrapped operation and handle its failure policy.

                Returns:
                    Dict[str, str] | None: Response mapping, or None after a retryable request failure.
                """
                nonlocal res

                try:
                    return f(*args, **kwargs)
                except RateLimitedError as msg:
                    log.warning(f'Ratelimited, waiting "{msg.delay + ratelimit_buffer}"s before trying again')
                    time.sleep(msg.delay + ratelimit_buffer)
                    return None
                except URLError as msg:
                    log.warning(msg)
                    return None

            if retries > 0:
                # Finite number of user-specified retries.
                for _ in range(retries):
                    if (res := call()) is not None:
                        return res
                else:
                    log.error(f'Retry attempt limit exceeded')
                    return None
            else:
                # Infinite retries.
                while (res := call()) is None:
                    time.sleep(retry_delay)

                return res

        return wrapper
    return decorator
