"""慢域名占满全局额度后，另一个域名拿什么读数，五行。

跑法：.venv/bin/python repro/per-origin-starvation.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os

import anyio
import httpcore


GOOD = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]


class HangingStream(httpcore.AsyncMockStream):
    async def read(self, max_bytes: int, timeout=None) -> bytes:
        await anyio.sleep(999)  # slow origin: never answers
        return b""  # pragma: no cover


class PerHostBackend(httpcore.AsyncMockBackend):
    def __init__(self) -> None:
        super().__init__(GOOD)
        self.connects: list = []

    async def connect_tcp(self, host, port, timeout=None, local_address=None,
                          socket_options=None):
        self.connects.append(host)
        if host == "slow.com":
            return HangingStream(GOOD, http2=False)
        return httpcore.AsyncMockStream(list(self._buffer), http2=False)


async def main() -> None:
    backend = PerHostBackend()
    pool = httpcore.AsyncConnectionPool(network_backend=backend, max_connections=2)
    async with pool:
        async with anyio.create_task_group() as tg:
            tg.start_soon(pool.request, "GET", "https://slow.com/1")
            tg.start_soon(pool.request, "GET", "https://slow.com/2")
            await anyio.sleep(0.1)
            print("pool:", repr(pool))
            print("origins connected so far:", backend.connects)

            t0 = anyio.current_time()
            try:
                await pool.request(
                    "GET", "https://fast.com/",
                    extensions={"timeout": {"pool": 0.2}},
                )
                print("fast.com succeeded")
            except BaseException as exc:  # noqa: BLE001
                print(
                    "fast.com after %.2fs:" % (anyio.current_time() - t0),
                    type(exc).__name__,
                    exc,
                )
            print("connections to fast.com so far:",
                  [c for c in backend.connects if c == "fast.com"])
            tg.cancel_scope.cancel()


anyio.run(main)
