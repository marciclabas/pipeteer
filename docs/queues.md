# Queues

Queues are how `pipeteer`'s pipelines communicate. In essence, a `Queue[T]` is an asynchronouse key-value store (mapping `str -> T`).

## Operations

Queues support the following operations:

- `async def push(key: str, value: T)`
- `async def pop(key: str) -> T`
- `async def read(key: str) -> T`
- `async def read_any() -> tuple[str, T]`
- `def items() -> AsyncIterable[tuple[str, T]]`
- (and a few more)

## Views

Queues can have two separate views:

- `ReadQueue[T]`: can be read and popped from, but not pushed to.
- `WriteQueue[T]`: can be pushed to, but not read or popped from.

In general, **`Queue`s are stored somewhere in disk or memory**, whilst *views may be transformations of the underlying `Queue`.*

Let's see how these are used by [pipelines](pipelines.md)!