#!/usr/bin/env python3
"""
async_responder_max_speed.py
High-performance UDP echo responder for small packets (64B) on Linux.
Multi-process + multi-async-worker design using SO_REUSEPORT.
"""

import asyncio
import socket
import argparse
import logging
import multiprocessing
import os

# Preallocate 64KB buffer to reuse for recv
RECV_BUFFER_SIZE = 65536

async def worker_loop(sock):
    loop = asyncio.get_running_loop()
    while True:
        try:
            data, addr = await loop.sock_recvfrom(sock, RECV_BUFFER_SIZE)
            await loop.sock_sendto(sock, data, addr)
        except Exception as e:
            logging.debug(f"Worker exception: {e}")

async def run_workers(sock, workers):
    tasks = [asyncio.create_task(worker_loop(sock)) for _ in range(workers)]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.info("Responder cancelled")
    finally:
        sock.close()

def responder_process(host, port, workers, rcvbuf, sndbuf):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Enable SO_REUSEPORT to allow multiple processes on same port
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.setblocking(False)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)

    # Log single startup message per process
    logging.info(f"Responder listening on {host}:{port} | pid={os.getpid()}")

    asyncio.run(run_workers(sock, workers))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--processes", type=int, default=os.cpu_count(), help="Number of parallel processes")
    parser.add_argument("--workers", type=int, default=4, help="Async workers per process")
    parser.add_argument("--rcvbuf", type=int, default=32*1024*1024)
    parser.add_argument("--sndbuf", type=int, default=32*1024*1024)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    procs = []
    for _ in range(args.processes):
        p = multiprocessing.Process(target=responder_process,
                                    args=(args.host, args.port, args.workers, args.rcvbuf, args.sndbuf))
        p.start()
        procs.append(p)

    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        logging.info("Stopping responder")
        for p in procs:
            p.terminate()
        for p in procs:
            p.join()

if __name__ == "__main__":
    main()
