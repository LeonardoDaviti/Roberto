class XAPIError(RuntimeError):
    """Base X API error."""


class UnauthorizedError(XAPIError):
    """Invalid or missing token."""


class ForbiddenError(XAPIError):
    """Authenticated but lacks sufficient access."""


class PaymentRequiredError(XAPIError):
    """Account credits are depleted (HTTP 402)."""


class RateLimitError(XAPIError):
    """Rate limited after retries."""


class NotFoundError(XAPIError):
    """Resource not found."""
