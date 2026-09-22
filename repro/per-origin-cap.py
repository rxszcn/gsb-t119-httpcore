"""加了每来源上限之后，谁被牺牲、上限有没有被突破。

跑法：.venv/bin/python repro/per-origin-cap.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os

import anyio
import httpcore


BUF = [
    b"HTTP/1.1 200 OK\r\n",
    b"Content-Type: plain/text\r\n",
    b"Content-Length: 13\r\n",
    b"\r\n",
    b"Hello, world!",
]


def dump(pool, label):
    print(label)
    for c in pool.connections:
        inner = c._connection
        stream = getattr(inner, "_network_stream", None)
        if stream is None and getattr(inner, "_connection", None) is not None:
            stream = inner._connection._network_stream
        print(
            "   ",
            repr(c),
            "| idle:", c.is_idle(),
            "| socket_closed:", getattr(stream, "_closed", None),
        )
    print("   ", repr(pool))


async def main() -> None:
    kwargs = {}
    try:
        httpcore.AsyncConnectionPool(
            network_backend=httpcore.AsyncMockBackend(BUF),
            max_connections=2,
            max_connections_per_origin=1,
        )
    except TypeError as exc:
        print("pool does not accept max_connections_per_origin:", exc)
    else:
        kwargs["max_connections_per_origin"] = 1

    pool = httpcore.AsyncConnectionPool(
        network_backend=httpcore.AsyncMockBackend(BUF * 6),
        max_connections=4,
        **kwargs,
    )
    async with pool:
        # b.com: one finished request, so one healthy IDLE connection.
        await pool.request("GET", "https://b.com/")
        dump(pool, "after b.com request:")

        # a.com: one in-flight request, holding its connection.
        cm = pool.stream("GET", "https://a.com/")
        held = await cm.__aenter__()
        dump(pool, "while holding a.com stream:")

        try:
            await pool.request(
                "GET", "https://a.com/", extensions={"timeout": {"pool": 0.3}}
            )
            print("second a.com request: SUCCEEDED")
        except BaseException as exc:  # noqa: BLE001
            print(f"second a.com request: {type(exc).__name__} ({exc})")

        dump(pool, "verdict -- b.com keepalive and the a.com connection count:")
        a_com = [c for c in pool.connections
                 if c.can_handle_request(httpcore.Origin(b"https", b"a.com", 443))]
        print("    a.com connections:", len(a_com))
        await cm.__aexit__(None, None, None)


anyio.run(main)
