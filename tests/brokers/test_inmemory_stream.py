import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from typing import Any

import pytest
from pydantic import BaseModel

from taskiq import (
    AckableMessage,
    AsyncBroker,
    AsyncTaskiqStream,
    InMemoryBroker,
    StreamNotFoundError,
    StreamNotSupportedError,
    StreamReader,
    TaskiqDepends,
)
from taskiq.message import BrokerMessage


class StreamItem(BaseModel):
    """Item yielded by streaming tasks."""

    value: int


async def collect(
    handle: AsyncTaskiqStream[Any] | StreamReader[Any],
) -> list[Any]:
    """Collect all items from a stream handle or reader."""
    items: list[Any] = []
    async for item in handle:
        items.append(item)
    return items


async def test_stream_yields_in_order() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def simple_stream(count: int) -> AsyncIterator[int]:
        for i in range(count):
            yield i

    handle = await simple_stream.kiq(5)

    assert await collect(handle) == [0, 1, 2, 3, 4]
    result = await handle.wait_result()
    assert not result.is_err
    await broker.wait_all()


async def test_stream_restores_item_types() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def model_stream() -> AsyncIterator[StreamItem]:
        # Raw wire payload, restored to StreamItem by the reader.
        yield {"value": 1}  # type: ignore[misc]
        yield {"value": 2}  # type: ignore[misc]

    handle = await model_stream.kiq()
    items = await collect(handle)

    assert items == [StreamItem(value=1), StreamItem(value=2)]
    assert all(isinstance(item, StreamItem) for item in items)


async def test_multiple_readers_read_independently() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def slow_stream() -> AsyncIterator[int]:
        for i in range(5):
            await asyncio.sleep(0.01)
            yield i

    handle = await slow_stream.kiq()
    first = handle.reader()
    second = handle.reader()

    first_items, second_items = await asyncio.gather(
        collect(first),
        collect(second),
    )

    assert first_items == [0, 1, 2, 3, 4]
    assert second_items == [0, 1, 2, 3, 4]
    assert first.error is None
    assert second.error is None
    # Readers keep independent positions.
    assert first.checkpoint == 5
    assert second.checkpoint == 5


async def test_reader_resumes_from_checkpoint() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def resume_stream() -> AsyncIterator[int]:
        for i in range(5):
            yield i

    handle = await resume_stream.kiq()
    await broker.wait_all()

    first = handle.reader()
    head = []
    async for item in first:
        head.append(item)
        if len(head) == 2:
            break
    await first.aclose()
    assert first.checkpoint == 2

    second = handle.reader(checkpoint=first.checkpoint)
    assert await collect(second) == [2, 3, 4]
    assert second.error is None

    # A fresh reader can replay the whole stream again.
    assert await collect(handle.reader()) == [0, 1, 2, 3, 4]


async def test_unsubscribing_one_reader_keeps_others() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def stream() -> AsyncIterator[int]:
        for i in range(4):
            await asyncio.sleep(0.01)
            yield i

    handle = await stream.kiq()
    full = handle.reader()
    partial = handle.reader()

    partial_items = []
    async for item in partial:
        partial_items.append(item)
        if len(partial_items) == 2:
            break
    await partial.aclose()

    assert await collect(full) == [0, 1, 2, 3]
    assert partial_items == [0, 1]


async def test_stream_error_keeps_items_and_reports_terminal_state() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def failing_stream() -> AsyncIterator[int]:
        yield 0
        yield 1
        raise ValueError("boom")

    handle = await failing_stream.kiq()
    reader = handle.reader()
    items = await collect(reader)

    assert items == [0, 1]
    assert reader.is_err
    assert isinstance(reader.error, ValueError)
    with pytest.raises(ValueError, match="boom"):
        reader.raise_for_error()

    result = await handle.wait_result()
    assert result.is_err
    assert isinstance(result.error, ValueError)


async def test_stream_timeout_keeps_produced_items() -> None:
    released = False
    broker = InMemoryBroker()

    @broker.task
    async def stuck_stream() -> AsyncIterator[int]:
        nonlocal released
        try:
            yield 0
            await asyncio.sleep(10)
        finally:
            released = True

    handle = await stuck_stream.kicker().with_labels(timeout=0.2).kiq()
    reader = handle.reader()
    items = await collect(reader)

    assert items == [0]
    assert reader.is_err
    assert reader.error is not None
    result = await handle.wait_result()
    assert result.is_err
    assert released


async def test_generator_and_dependencies_are_released_on_error() -> None:
    released: set[str] = set()

    async def tracking_dependency() -> AsyncIterator[str]:
        try:
            yield "dep"
        finally:
            released.add("dependency")

    broker = InMemoryBroker()

    @broker.task
    async def stream_with_dep(
        dep: str = TaskiqDepends(tracking_dependency),  # type: ignore[assignment]
    ) -> AsyncIterator[int]:
        try:
            yield 1
            raise RuntimeError("boom")
        finally:
            released.add("generator")

    handle = await stream_with_dep.kiq()
    reader = handle.reader()
    await collect(reader)

    await broker.wait_all()
    assert released == {"dependency", "generator"}
    assert reader.is_err


async def test_late_subscriber_replays_finished_stream() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def stream() -> AsyncIterator[int]:
        yield 1
        yield 2

    handle = await stream.kiq()
    await broker.wait_all()

    items = await collect(handle)
    assert items == [1, 2]


async def test_await_inplace_replays_buffer() -> None:
    broker = InMemoryBroker(await_inplace=True)

    @broker.task
    async def stream() -> AsyncIterator[int]:
        yield 7
        yield 8

    handle = await stream.kiq()
    assert await handle.is_ready()
    assert await collect(handle) == [7, 8]


async def test_normal_tasks_still_work_alongside_streams() -> None:
    broker = InMemoryBroker()

    @broker.task
    async def plain_task() -> str:
        return "plain"

    @broker.task
    async def stream_task() -> AsyncIterator[int]:
        yield 1

    plain = await plain_task.kiq()
    stream = await stream_task.kiq()

    assert (await plain.wait_result()).return_value == "plain"
    assert await collect(stream) == [1]


async def test_subscribe_to_unknown_stream_raises() -> None:
    broker = InMemoryBroker()

    with pytest.raises(StreamNotFoundError):
        async for _ in broker.stream_backend.subscribe("missing"):
            pass


async def test_stream_eviction() -> None:
    broker = InMemoryBroker(max_stored_streams=1)

    @broker.task
    async def stream() -> AsyncIterator[int]:
        yield 1

    first = await stream.kiq()
    await collect(first)
    second = await stream.kiq()
    await collect(second)

    with pytest.raises(StreamNotFoundError):
        async for _ in broker.stream_backend.subscribe(first.task_id):
            pass


class _BrokerWithoutStreams(AsyncBroker):
    """Broker that doesn't configure a stream backend."""

    async def kick(self, message: BrokerMessage) -> None:
        pass

    def listen(self) -> AsyncGenerator[bytes | AckableMessage, None]:
        raise RuntimeError("cannot listen")


async def test_stream_task_unsupported_by_broker() -> None:
    broker = _BrokerWithoutStreams()

    @broker.task
    async def stream() -> AsyncIterator[int]:
        yield 1

    with pytest.raises(StreamNotSupportedError):
        await stream.kiq()
