import asyncio
import functools as ft
import typing
from typing import Any, Awaitable, Callable, TypeVar

ResultT = TypeVar('ResultT')


async def guard(coro: Awaitable[ResultT]) -> ResultT:
    """
    Guards a coroutine from cancellation ignoring `asyncio.CancelledError`.
    Acts similar to `asyncio.shield` but wait for the shielded coroutine to be finished.
    """

    inner_fut = asyncio.ensure_future(coro)

    cancelled = False
    while True:
        try:
            result = await asyncio.shield(inner_fut)
        except asyncio.CancelledError:
            cancelled = True
        else:
            break

    if cancelled:
        raise asyncio.CancelledError

    return result


AsyncFuncT = TypeVar('AsyncFuncT', bound=Callable[..., Awaitable[Any]])


def guarded(func: AsyncFuncT) -> AsyncFuncT:
    """
    Guard decorator.
    """

    @ft.wraps(func)
    async def decorator(*args: Any, **kwargs: Any) -> Any:
        return await guard(func(*args, **kwargs))

    return typing.cast(AsyncFuncT, decorator)
