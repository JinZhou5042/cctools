#!/usr/bin/env python3

import http.server

from ndcctools.taskvine.datavine.controller.admission import (
    BoundedThreadingHTTPServer,
    ByteServingAdmission,
)


def main():
    admission = ByteServingAdmission(2, 10)
    assert admission.acquire(4)
    assert admission.acquire(6)
    assert not admission.acquire(0)
    assert not admission.acquire(1)
    admission.release(4, completed=True)
    assert admission.acquire(1)
    admission.release(6, completed=False)
    admission.release(1, completed=True)
    snapshot = admission.snapshot()
    assert snapshot == {
        "active": 0,
        "active_capacity": 2,
        "active_high_water": 2,
        "inflight_bytes": 0,
        "inflight_byte_capacity": 10,
        "inflight_byte_high_water": 10,
        "admitted": 3,
        "rejected": 2,
        "bytes_served": 5,
    }
    for action, error_type in (
        (lambda: admission.acquire(-1), ValueError),
        (lambda: admission.release(1, True), RuntimeError),
        (lambda: ByteServingAdmission(0, 1), ValueError),
        (lambda: ByteServingAdmission(1, 0), ValueError),
    ):
        try:
            action()
        except error_type:
            pass
        else:
            raise AssertionError(f"expected {error_type.__name__}")

    server = BoundedThreadingHTTPServer(
        ("127.0.0.1", 0), http.server.BaseHTTPRequestHandler, 3
    )
    try:
        assert server.admission_snapshot() == {
            "active": 0,
            "active_capacity": 3,
            "active_high_water": 0,
            "rejected": 0,
        }
    finally:
        server.server_close()
    try:
        BoundedThreadingHTTPServer(
            ("127.0.0.1", 0), http.server.BaseHTTPRequestHandler, 0
        )
    except ValueError:
        pass
    else:
        raise AssertionError("accepted zero request capacity")

    print("DataVine Controller admission contract PASS")


if __name__ == "__main__":
    main()
