"""Tests for the Kubernetes port-forward socket bridge."""

from __future__ import annotations

import socket
import threading
import time

from execution_plane.worker_manager.vanilla_k8s.forward import forward_socket


class _DelayedRemote:
    def __init__(self, wrapped: socket.socket) -> None:
        self._wrapped = wrapped
        self.recv_entered = threading.Event()
        self.recv_calls = 0

    def __getattr__(self, name: str) -> object:
        return getattr(self._wrapped, name)

    def recv(self, bufsize: int) -> bytes:
        self.recv_entered.set()
        self.recv_calls += 1
        if self.recv_calls == 1:
            time.sleep(0.05)
            return b""
        return self._wrapped.recv(bufsize)


def test_forward_socket_cleanup_does_not_reset_kubernetes_proxy() -> None:
    remote_socket, kubernetes_socket = socket.socketpair()
    remote = _DelayedRemote(remote_socket)
    proxy_errors: list[BaseException] = []

    def kubernetes_proxy() -> None:
        try:
            while kubernetes_socket.recv(1024):
                pass
        except BaseException as exc:
            proxy_errors.append(exc)
        finally:
            kubernetes_socket.close()

    proxy_thread = threading.Thread(target=kubernetes_proxy)
    proxy_thread.start()

    with forward_socket(remote) as address:  # type: ignore[arg-type]
        host, port = address.split(":", 1)
        with socket.create_connection((host, int(port)), timeout=1):
            kubernetes_socket.sendall(b"pending remote data")
            assert remote.recv_entered.wait(timeout=1)

    proxy_thread.join(timeout=1)

    assert not proxy_thread.is_alive()
    assert proxy_errors == []
