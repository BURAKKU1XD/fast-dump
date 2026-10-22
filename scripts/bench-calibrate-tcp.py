#!/usr/bin/env python3
"""Measure TCP RTT and goodput inside the namespace configured by bench-netem.sh."""

import argparse
import json
import os
from pathlib import Path
import queue
import socket
import statistics
import threading
import time


BLOCK_BYTES = 1024 * 1024
SOCKET_TIMEOUT = 120.0
PAYLOAD = bytes([0xA5]) * BLOCK_BYTES


def configure(connection):
    connection.settimeout(SOCKET_TIMEOUT)
    connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    # Размеры socket buffer не фиксируем: сохраняем TCP autotuning и отдельно
    # записываем фактические значения, чтобы отличать его предел от netem rate.


def socket_info(connection):
    info = {
        "send_buffer_bytes": connection.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF),
        "receive_buffer_bytes": connection.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF),
        "tcp_max_segment_bytes": connection.getsockopt(socket.IPPROTO_TCP, socket.TCP_MAXSEG),
    }
    if hasattr(socket, "TCP_CONGESTION"):
        info["congestion_control"] = connection.getsockopt(
            socket.IPPROTO_TCP, socket.TCP_CONGESTION, 64
        ).rstrip(b"\0").decode("ascii", "replace")
    return info


def listening_socket():
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.settimeout(SOCKET_TIMEOUT)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    # listen завершён до connect: нет sleep/polling и гонки готовности TCP.
    return listener


def receive_exact(connection, count, buffer):
    received = 0
    view = memoryview(buffer)
    while received < count:
        size = connection.recv_into(view[: min(count - received, len(view))])
        if size == 0:
            raise RuntimeError(f"premature EOF after {received} of {count} bytes")
        received += size
    return received


def send_payload(connection, count):
    remaining = count
    view = memoryview(PAYLOAD)
    while remaining:
        piece = view[: min(remaining, len(view))]
        connection.sendall(piece)
        remaining -= len(piece)
    connection.shutdown(socket.SHUT_WR)


def receive_payload(connection, count):
    received = receive_exact(connection, count, bytearray(BLOCK_BYTES))
    if connection.recv(1) != b"":
        raise RuntimeError("payload exceeded requested byte count")
    return received


def percentile(samples, fraction):
    ordered = sorted(samples)
    return ordered[round((len(ordered) - 1) * fraction)]


def measure_rtt(samples):
    listener = listening_socket()
    errors = queue.Queue()

    def server():
        try:
            with listener.accept()[0] as connection:
                configure(connection)
                connection.sendall(b"R")
                while True:
                    message = connection.recv(1)
                    if not message or message == b"Q":
                        break
                    connection.sendall(message)
        except BaseException as error:
            errors.put(error)

    worker = threading.Thread(target=server, daemon=True)
    worker.start()
    latencies = []
    try:
        with socket.create_connection(listener.getsockname(), SOCKET_TIMEOUT) as client:
            configure(client)
            if client.recv(1) != b"R":
                raise RuntimeError("RTT server did not become ready")
            for index in range(samples + 16):
                started = time.perf_counter_ns()
                client.sendall(b"P")
                if client.recv(1) != b"P":
                    raise RuntimeError("invalid TCP echo")
                elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
                if index >= 16:
                    latencies.append(elapsed_ms)
            client.sendall(b"Q")
    finally:
        listener.close()
        worker.join(SOCKET_TIMEOUT + 1)
    if worker.is_alive():
        raise RuntimeError("RTT worker did not stop")
    if not errors.empty():
        raise errors.get()
    return {
        "samples": samples,
        "warmup_samples": 16,
        "min_ms": min(latencies),
        "median_ms": statistics.median(latencies),
        "mean_ms": statistics.mean(latencies),
        "p95_ms": percentile(latencies, 0.95),
        "max_ms": max(latencies),
    }


def measure_transfer(mode, byte_count):
    listener = listening_socket()
    errors = queue.Queue()
    connection_info = {}
    start = {}

    def begin():
        start["wall"] = time.perf_counter()
        start["cpu"] = time.process_time()

    barrier = threading.Barrier(2, action=begin, timeout=SOCKET_TIMEOUT)

    def endpoint(connection, role):
        configure(connection)
        barrier.wait()
        sends = mode == "bidirectional" or (mode == "forward") == (role == "client")
        receives = mode == "bidirectional" or not sends
        if sends and receives:
            sender_errors = queue.Queue()

            def sender():
                try:
                    send_payload(connection, byte_count)
                except BaseException as error:
                    sender_errors.put(error)
                    try:
                        connection.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass

            sender_thread = threading.Thread(target=sender, daemon=True)
            sender_thread.start()
            try:
                receive_payload(connection, byte_count)
            finally:
                sender_thread.join(SOCKET_TIMEOUT + 1)
            if sender_thread.is_alive():
                raise RuntimeError("duplex sender did not stop")
            if not sender_errors.empty():
                raise sender_errors.get()
        elif sends:
            send_payload(connection, byte_count)
            if connection.recv(1) != b"K":
                raise RuntimeError("receiver did not acknowledge complete payload")
        else:
            receive_payload(connection, byte_count)
            connection.sendall(b"K")
        connection_info[role] = socket_info(connection)

    def server():
        try:
            with listener.accept()[0] as connection:
                endpoint(connection, "server")
        except BaseException as error:
            barrier.abort()
            errors.put(error)

    worker = threading.Thread(target=server, daemon=True)
    worker.start()
    try:
        with socket.create_connection(listener.getsockname(), SOCKET_TIMEOUT) as client:
            try:
                endpoint(client, "client")
            except BaseException:
                barrier.abort()
                raise
    finally:
        listener.close()
        worker.join(SOCKET_TIMEOUT + 1)
    if worker.is_alive():
        raise RuntimeError("transfer server did not stop")
    if not errors.empty():
        raise errors.get()
    elapsed = time.perf_counter() - start["wall"]
    cpu_elapsed = time.process_time() - start["cpu"]
    directions = 2 if mode == "bidirectional" else 1
    return {
        "mode": mode,
        "tcp_connections": 1,
        "bytes_per_direction": byte_count,
        "directions": directions,
        "elapsed_seconds": elapsed,
        "process_cpu_seconds": cpu_elapsed,
        "per_direction_goodput_gbps": byte_count * 8 / elapsed / 1_000_000_000,
        "aggregate_goodput_gbps": byte_count * directions * 8 / elapsed / 1_000_000_000,
        "socket_info_after_transfer": connection_info,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bytes-mib", type=int, default=256)
    parser.add_argument("--rtt-samples", type=int, default=128)
    parser.add_argument("--rate-label", default="unspecified")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.bytes_mib < 1 or args.rtt_samples < 1:
        parser.error("sample counts and transfer size must be positive")
    report = {
        "rate_label": args.rate_label,
        "rate_scope": "shared_loopback_egress",
        "network_namespace": os.readlink("/proc/self/ns/net"),
        "clock": "perf_counter",
        "payload_block_bytes": BLOCK_BYTES,
        "traffic_source": "TCP sockets; no application sleeps or artificial chunk delays",
        "rtt": measure_rtt(args.rtt_samples),
        "transfers": [],
    }
    print(f"TCP RTT median={report['rtt']['median_ms']:.3f} ms p95={report['rtt']['p95_ms']:.3f} ms", flush=True)
    for mode in ("forward", "reverse", "bidirectional"):
        measurement = measure_transfer(mode, args.bytes_mib * 1024 * 1024)
        report["transfers"].append(measurement)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"{mode}: {measurement['aggregate_goodput_gbps']:.3f} Gbit/s aggregate; "
              f"{measurement['per_direction_goodput_gbps']:.3f} Gbit/s per direction; "
              f"{measurement['elapsed_seconds']:.3f} s", flush=True)


if __name__ == "__main__":
    main()
