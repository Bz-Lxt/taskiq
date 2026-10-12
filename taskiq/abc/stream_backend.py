from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, TypeVar

_ItemType = TypeVar("_ItemType")


class StreamEventKind(Enum):
    """Kind of a stream event."""

    ITEM = auto()
    END = auto()


@dataclass(frozen=True, slots=True)
class StreamEvent(Generic[_ItemType]):
    """An event delivered to stream readers.

    Item events carry one yielded value and its monotonic
    sequence number. The end event marks the terminal state
    of the stream; if the generator failed, ``error`` holds
    the exception.
    """

    seq: int
    kind: StreamEventKind
    value: _ItemType | None = None
    error: BaseException | None = None


class AsyncStreamBackend(ABC, Generic[_ItemType]):
    """
    Abstract backend for streaming tasks.

    The worker side publishes yielded values together with
    their sequence numbers using ``open_stream``, ``add_item``
    and ``finish_stream``. The client side independently subscribes
    to a stream and can replay it from any sequence number.
    """

    async def startup(self) -> None:
        """Do something when starting the backend."""

    async def shutdown(self) -> None:
        """Do something on shutdown."""

    @abstractmethod
    async def open_stream(self, task_id: str) -> None:
        """
        Create a stream for a task execution.

        This method is called before the task starts executing,
        so late subscribers always find the stream.

        :param task_id: id of the task.
        """

    @abstractmethod
    async def add_item(self, task_id: str, seq: int, item: _ItemType) -> None:
        """
        Publish one yielded value.

        :param task_id: id of the task.
        :param seq: monotonic sequence number, starting from 0.
        :param item: yielded value.
        """

    @abstractmethod
    async def finish_stream(
        self,
        task_id: str,
        error: BaseException | None = None,
    ) -> None:
        """
        Mark the stream as finished.

        Already published items must remain available.
        If ``error`` is not None the stream finished with
        an exception or a timeout.

        :param task_id: id of the task.
        :param error: terminal exception or None on success.
        """

    @abstractmethod
    def subscribe(
        self,
        task_id: str,
        start_seq: int = 0,
    ) -> AsyncGenerator[StreamEvent[_ItemType], None]:
        """
        Subscribe to a stream.

        Buffered items with sequence numbers greater or equal to
        ``start_seq`` are replayed first, then the subscriber
        follows the live stream. The last event is always the
        end event carrying the terminal state.

        Each subscription keeps its own position and does not
        affect other subscribers.

        :param task_id: id of the task.
        :param start_seq: sequence number to start reading from.
        :yields: stream events in sequence order.
        """
        raise NotImplementedError
