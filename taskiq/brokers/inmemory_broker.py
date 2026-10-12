import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from taskiq.abc.broker import AsyncBroker
from taskiq.abc.middleware import TaskiqMiddleware
from taskiq.abc.result_backend import AsyncResultBackend, TaskiqResult
from taskiq.abc.stream_backend import (
    AsyncStreamBackend,
    StreamEvent,
    StreamEventKind,
)
from taskiq.depends.progress_tracker import TaskProgress
from taskiq.events import TaskiqEvents
from taskiq.exceptions import (
    StreamClosedError,
    StreamNotFoundError,
    UnknownTaskError,
)
from taskiq.message import BrokerMessage
from taskiq.receiver import Receiver
from taskiq.utils import maybe_awaitable

logger = logging.getLogger(__name__)

_ReturnType = TypeVar("_ReturnType")

# Sentinel value for streams that are still running.
_STREAM_RUNNING = object()


class InmemoryResultBackend(AsyncResultBackend[_ReturnType]):
    """
    Inmemory result backend.

    This resultbackend is intended to be used only
    with inmemory broker.

    It stores all results in a dict in memory.
    """

    def __init__(self, max_stored_results: int = 100) -> None:
        self.max_stored_results = max_stored_results
        self.results: OrderedDict[str, TaskiqResult[_ReturnType]] = OrderedDict()
        self.progress: OrderedDict[str, TaskProgress[Any]] = OrderedDict()

    async def set_result(self, task_id: str, result: TaskiqResult[_ReturnType]) -> None:
        """
        Sets result.

        This method is used to store result of an execution in a
        results dict. But also it removes previous results
        to keep memory footprint as low as possible.

        :param task_id: id of a task.
        :param result: result of an execution.
        """
        if (
            self.max_stored_results != -1
            and len(self.results) >= self.max_stored_results
        ):
            self.results.popitem(last=False)
        self.results[task_id] = result

    async def is_result_ready(self, task_id: str) -> bool:
        """
        Checks whether result is ready.

        Readiness means that result with this task_id is
        present in results dict.

        :param task_id: id of a task to check.
        :return: True if ready.
        """
        return task_id in self.results

    async def get_result(
        self,
        task_id: str,
        with_logs: bool = False,
    ) -> TaskiqResult[_ReturnType]:
        """
        Get result of a task.

        This method is used to get result
        from result dict.

        It throws exception in case if
        result dict doesn't have a value
        for task_id.

        :param task_id: id of a task.
        :param with_logs: this option is ignored.
        :return: result of a task execution.
        """
        return self.results[task_id]

    async def set_progress(
        self,
        task_id: str,
        progress: TaskProgress[Any],
    ) -> None:
        """
        Set progress of task execution.

        :param task_id: task id
        :param progress: task execution progress
        """
        if (
            self.max_stored_results != -1
            and len(self.progress) >= self.max_stored_results
        ):
            self.progress.popitem(last=False)

        self.progress[task_id] = progress

    async def get_progress(
        self,
        task_id: str,
    ) -> TaskProgress[Any] | None:
        """
        Get progress of task execution.

        :param task_id: task id
        :return: progress or None
        """
        return self.progress.get(task_id)


class _StreamState:
    """Buffered state of one streaming task execution."""

    __slots__ = ("cond", "error", "items", "readers")

    def __init__(self) -> None:
        self.items: list[Any] = []
        self.error: Any = _STREAM_RUNNING
        self.cond = asyncio.Condition()
        self.readers = 0

    @property
    def is_running(self) -> bool:
        """True until the terminal state is published."""
        return self.error is _STREAM_RUNNING


class InmemoryStreamBackend(AsyncStreamBackend[Any]):
    """
    Inmemory stream backend.

    This stream backend is intended to be used with the inmemory broker.
    Published items are buffered in memory and can be replayed by
    any number of independent subscribers, each starting at its own
    sequence number.
    """

    def __init__(self, max_stored_streams: int = 100) -> None:
        self.max_stored_streams = max_stored_streams
        self.streams: OrderedDict[str, _StreamState] = OrderedDict()

    async def open_stream(self, task_id: str) -> None:
        """
        Create an empty running stream.

        Finished streams without active readers are evicted first to
        keep memory footprint bounded.

        :param task_id: id of a task.
        """
        if (
            self.max_stored_streams != -1
            and len(self.streams) >= self.max_stored_streams
        ):
            await self._evict_stream()
        self.streams[task_id] = _StreamState()

    async def add_item(self, task_id: str, seq: int, item: Any) -> None:
        """
        Append one item to the stream and wake all subscribers.

        :param task_id: id of a task.
        :param seq: item sequence number.
        :param item: yielded value.
        """
        state = self.streams.get(task_id)
        if state is None:
            logger.debug("Stream for task %s was evicted, dropping item.", task_id)
            return
        async with state.cond:
            state.items.append(item)
            state.cond.notify_all()

    async def finish_stream(
        self,
        task_id: str,
        error: BaseException | None = None,
    ) -> None:
        """
        Mark the stream as finished and wake all subscribers.

        Buffered items stay available for late subscribers.

        :param task_id: id of a task.
        :param error: terminal exception or None on success.
        """
        state = self.streams.get(task_id)
        if state is None:
            return
        async with state.cond:
            state.error = error
            state.cond.notify_all()

    async def subscribe(
        self,
        task_id: str,
        start_seq: int = 0,
    ) -> AsyncGenerator[StreamEvent[Any], None]:
        """
        Subscribe to the stream starting from ``start_seq``.

        Buffered items are replayed first, then new items are
        delivered as they are produced. The subscription ends
        with an end event carrying the terminal exception.

        :param task_id: id of a task.
        :param start_seq: sequence number to start reading from.
        :yields: stream events.
        :raises StreamNotFoundError: if there is no stream for task_id.
        :raises ValueError: if start_seq is negative.
        """
        if start_seq < 0:
            raise ValueError("start_seq cannot be negative.")
        state = self.streams.get(task_id)
        if state is None:
            raise StreamNotFoundError(task_id=task_id)
        state.readers += 1
        seq = start_seq
        try:
            while True:
                event: StreamEvent[Any] | None = None
                async with state.cond:
                    if seq < len(state.items):
                        event = StreamEvent(
                            seq=seq,
                            kind=StreamEventKind.ITEM,
                            value=state.items[seq],
                        )
                        seq += 1
                    elif not state.is_running:
                        event = StreamEvent(
                            seq=seq,
                            kind=StreamEventKind.END,
                            error=(
                                None if state.error is _STREAM_RUNNING else state.error
                            ),
                        )
                    else:
                        await state.cond.wait()
                if event is not None:
                    yield event
                    if event.kind == StreamEventKind.END:
                        return
        finally:
            state.readers -= 1

    async def shutdown(self) -> None:
        """Wake up subscribers waiting on unfinished streams."""
        for task_id, state in self.streams.items():
            async with state.cond:
                if state.is_running:
                    state.error = StreamClosedError(task_id=task_id)
                state.cond.notify_all()

    async def _evict_stream(self) -> None:
        """Evict one stream, preferring finished streams without readers."""
        for candidate_id, state in self.streams.items():
            if not state.is_running and state.readers == 0:
                self.streams.pop(candidate_id)
                return
        evicted_id, state = self.streams.popitem(last=False)
        async with state.cond:
            state.error = StreamClosedError(task_id=evicted_id)
            state.cond.notify_all()


class InMemoryBroker(AsyncBroker):
    """
    This broker is used to execute tasks without sending them elsewhere.

    It's useful for local development, if you don't want to setup real broker.
    """

    def __init__(
        self,
        sync_tasks_pool_size: int = 4,
        max_stored_results: int = 100,
        max_stored_streams: int = 100,
        cast_types: bool = True,
        max_async_tasks: int = 30,
        max_async_tasks_jitter: int = 0,
        propagate_exceptions: bool = True,
        await_inplace: bool = False,
    ) -> None:
        super().__init__()
        self.result_backend: InmemoryResultBackend[Any] = InmemoryResultBackend(
            max_stored_results=max_stored_results,
        )
        self.stream_backend: InmemoryStreamBackend = InmemoryStreamBackend(
            max_stored_streams=max_stored_streams,
        )
        self.executor = ThreadPoolExecutor(sync_tasks_pool_size)
        self.receiver = Receiver(
            broker=self,
            executor=self.executor,
            validate_params=cast_types,
            max_async_tasks=max_async_tasks,
            max_async_tasks_jitter=max_async_tasks_jitter,
            propagate_exceptions=propagate_exceptions,
        )
        self.await_inplace = await_inplace
        self._running_tasks: set[asyncio.Task[Any]] = set()

    async def kick(self, message: BrokerMessage) -> None:
        """
        Kicking task.

        This method just executes given task.

        :param message: incoming message.

        :raises TaskiqError: if someone wants to kick unknown task.
        """
        target_task = self.find_task(message.task_name)
        if target_task is None:
            raise UnknownTaskError(task_name=message.task_name)

        if getattr(target_task, "is_stream", False):
            # Open the stream before scheduling execution so the handle returned
            # by kiq can always subscribe, even when the task finishes instantly.
            await self.stream_backend.open_stream(message.task_id)

        receiver_cb = self.receiver.callback(message=message.message)
        if self.await_inplace:
            await receiver_cb
            return

        task = asyncio.create_task(receiver_cb)
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    def listen(self) -> AsyncGenerator[bytes, None]:
        """
        Inmemory broker cannot listen.

        This method throws RuntimeError if you call it.
        Because inmemory broker cannot really listen to any of tasks.

        :raises RuntimeError: if this method is called.
        """
        raise RuntimeError("Inmemory brokers cannot listen.")

    async def wait_all(self) -> None:
        """
        Wait for all currently running tasks to complete.

        Useful when used in testing and you need to await all sent tasks
        before asserting results.
        """
        to_await = list(self._running_tasks)
        for task in to_await:
            await task

    async def startup(self) -> None:
        """Runs startup events for client and worker side."""
        for event in (TaskiqEvents.CLIENT_STARTUP, TaskiqEvents.WORKER_STARTUP):
            for handler in self.event_handlers.get(event, []):
                await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.startup != TaskiqMiddleware.startup:
                await maybe_awaitable(middleware.startup())

        await self.result_backend.startup()
        await self.stream_backend.startup()

    async def shutdown(self) -> None:
        """Runs shutdown events for client and worker side."""
        for event in (TaskiqEvents.CLIENT_SHUTDOWN, TaskiqEvents.WORKER_SHUTDOWN):
            for handler in self.event_handlers.get(event, []):
                await maybe_awaitable(handler(self.state))

        for middleware in self.middlewares:
            if middleware.__class__.shutdown != TaskiqMiddleware.shutdown:
                await maybe_awaitable(middleware.shutdown())

        await self.stream_backend.shutdown()
        await self.result_backend.shutdown()
        self.executor.shutdown()
