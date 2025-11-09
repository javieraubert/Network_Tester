#!/usr/bin/env python3
"""
udp_responder_hs.py
High-speed UDP echo responder for small packets (64B) on Linux.

Design:
- Multi-process (uno por CPU) + SO_REUSEPORT
- Multi-thread workers por proceso
- Recvfrom/sendto direct, sin asyncio overhead
- Large socket buffers, preallocated memory
- Optimized para saturar enlaces con paquetes pequeños
"""

import socket
import argparse
import logging
import os
import multiprocessing
import threading

# Preallocate buffer for recv
RECV_BUFFER_SIZE = 65536

def worker_loop(sock):
    buf = bytearray(RECV_BUFFER_SIZE)
    while True:
        try:
            nbytes, addr = sock.recvfrom_into(buf)
            sock.sendto(buf[:nbytes], addr)
        except Exception as e:
            logging.debug(f"Worker exception: {e}")

def process_main(host, port, workers, rcvbuf, sndbuf):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)

    logging.info(f"Responder listening on {host}:{port} | pid={os.getpid()}")

    threads = []
    for _ in range(workers):
        t = threading.Thread(target=worker_loop, args=(sock,), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--processes", type=int, default=os.cpu_count())
    parser.add_argument("--workers", type=int, default=2, help="Threads per process")
    parser.add_argument("--rcvbuf", type=int, default=32*1024*1024)
    parser.add_argument("--sndbuf", type=int, default=32*1024*1024)
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    procs = []
    for _ in range(args.processes):
        p = multiprocessing.Process(target=process_main,
                                    args=(args.host, args.port, args.workers,
                                          args.rcvbuf, args.sndbuf))
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
