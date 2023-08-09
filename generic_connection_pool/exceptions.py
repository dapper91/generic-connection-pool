
class BaseError(Exception):
    """
    Base package exception.
    """


class ConnectionPoolClosedError(BaseError):
    """
    Connection pool already closed.
    """


class ConnectionPoolIsFull(BaseError):
    """
    Indicates that a pool is full of connections.
    """


class ConnectionPoolNotFound(BaseError):
    """
    Connection pool not found.
    """
