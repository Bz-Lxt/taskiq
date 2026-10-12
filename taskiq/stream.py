import sys
from collections.abc import AsyncGenerator
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from taskiq.abc.stream_backend import StreamEventKind
from taskiq.compat import parse_obj_as
from taskiq.task import AsyncTaskiqTask

if TYPE_CHECKING:  # pragma: no cover
    from taskiq.abc.result_backend import AsyncResultBackend
    from taskiq.abc.stream_backend import AsyncStreamBackend
    from taskiq.result import TaskiqResult

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

logger = getLogger("taskiq.stream")

_ItemType = TypeVar("_ItemType")


class StreamReader(Generic[_ItemType]):
    """
    Independent reader of a task stream.

    Each reader keeps its own position in the stream, so several
    readers can consume the same stream concurrently and any reader
    can resume reading from its last ``checkpoint``.

    Iterate over the reader to get yielded values in order. After
    the iteration ends, inspect ``error`` to learn the terminal
    state of the task.
    """

    def __init__(
        self,
        task_id: str,
        stream_backend: "AsyncStreamBackend[_ItemType]",
        item_type: Any,
        checkpoint: int = 0,
    ) -> None:
        self.task_id = task_id
        self.checkpoint = checkpoint
        self.error: BaseException | None = None
        self._stream_backend = stream_backend
        self._item_type = item_type
        self._events: AsyncGenerator[Any, None] | None = None

    def __aiter__(self) -> "Self":
        return self

    async def __anext__(self) -> _ItemType:
        if self._events is None:
            self._events = self._stream_backend.subscribe(
                self.task_id,
                start_seq=self.checkpoint,
            )
        event = await self._events.__anext__()
        if event.kind == StreamEventKind.END:
            self.error = event.error
            raise StopAsyncIteration
        self.checkpoint = event.seq + 1
        return self._cast_value(event.value)

    async def aclose(self) -> None:
        """
        Stop reading and release the subscription.

        This does not affect other readers or the task itself.
        """
        if self._events is not None:
            await self._events.aclose()
            self._events = None

    @property
    def is_err(self) -> bool:
        """True if the stream finished with an error or a timeout."""
        return self.error is not None

    def raise_for_error(self) -> "StreamReader[_ItemType]":
        """
        Raise the terminal exception if there is one.

        :raises BaseException: terminal exception of the task.
        :return: the reader.
        """
        if self.error is not None:
            raise self.error
        return self

    def _cast_value(self, value: Any) -> _ItemType:
        if self._item_type is None:
            return value
        try:
            return parse_obj_as(self._item_type, value)
        except ValueError:
            logger.warning(
                "Cannot parse stream item into %s",
                self._item_type,
            )
            return value


class AsyncTaskiqStream(Generic[_ItemType]):
    """
    Handle of a streaming task.

    Use ``reader()`` to create independent readers, each with its
    own checkpoint, or iterate over the handle directly for a
    single default reader.

    The terminal result of the task is still stored in the result
    backend and can be awaited with ``wait_result``.
    """

    def __init__(
        self,
        task_id: str,
        result_backend: "AsyncResultBackend[Any]",
        stream_backend: "AsyncStreamBackend[_ItemType]",
        item_type: Any = None,
    ) -> None:
        self.task_id = task_id
        self._stream_backend = stream_backend
        self._result_task: AsyncTaskiqTask[Any] = AsyncTaskiqTask(
            task_id=task_id,
            result_backend=result_backend,
            return_type=None,
        )
        self._item_type = item_type
        self._default_reader: StreamReader[_ItemType] | None = None

    def reader(self, checkpoint: int = 0) -> StreamReader[_ItemType]:
        """
        Create a new independent reader.

        :param checkpoint: sequence number to start reading from.
            Pass another reader's ``checkpoint`` to resume reading.
        :return: new stream reader.
        """
        return StreamReader(
            task_id=self.task_id,
            stream_backend=self._stream_backend,
            item_type=self._item_type,
            checkpoint=checkpoint,
        )

    def __aiter__(self) -> StreamReader[_ItemType]:
        if self._default_reader is None:
            self._default_reader = self.reader()
        return self._default_reader

    async def is_ready(self) -> bool:
        """Check if the terminal result of the task is saved."""
        return await self._result_task.is_ready()

    async def get_result(
        self,
        with_logs: bool = False,
    ) -> "TaskiqResult[Any]":
        """Get the terminal result of the task."""
        return await self._result_task.get_result(with_logs=with_logs)

    async def wait_result(
        self,
        check_interval: float = 0.2,
        timeout: float = -1.0,
        with_logs: bool = False,
    ) -> "TaskiqResult[Any]":
        """Wait until the terminal result of the task is saved."""
        return await self._result_task.wait_result(
            check_interval=check_interval,
            timeout=timeout,
            with_logs=with_logs,
        )
