
class BaseError(Exception):
    """
    Base package exception.
    """


class ConnectionPoolClosedError(BaseError):
    """
    Connection pool already closed.
    """
