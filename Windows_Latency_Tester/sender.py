#!/usr/bin/env python3
"""
high_perf_udp_sender_win_live.py
High-performance UDP traffic generator for Windows with live stats.
"""

import argparse
import socket
import struct
import time
import os
import logging
from typing import List, Set
import threading
import statistics

HEADER_FMT = "!Qd"  # seq (uint64), timestamp (double)
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# ---------------- pattern generation ----------------
def pattern_bytes_from_spec(payload_size: int, spec: str) -> bytes:
    spec = spec.strip()
    if spec == "zeros":
        return bytes(payload_size)
    if spec == "ones":
        return bytes([0xFF]) * payload_size
    if spec == "alt01":
        return bytes((0x01 if i % 2 == 0 else 0x00) for i in range(payload_size))
    if spec == "alt10":
        return bytes((0x00 if i % 2 == 0 else 0x01) for i in range(payload_size))
    if spec == "prbs":
        out = bytearray(payload_size)
        state = 0xA5A5A5A5
        for i in range(payload_size):
            x = state
            x ^= (x << 13) & 0xFFFFFFFF
            x ^= (x >> 17) & 0xFFFFFFFF
            x ^= (x << 5) & 0xFFFFFFFF
            state = x
            out[i] = x & 0xFF
        return bytes(out)
    if spec == "random":
        return os.urandom(payload_size)
    if spec.startswith("ascii:"):
        txt = spec[len("ascii:"):]
        b = txt.encode("utf-8")
        return (b * ((payload_size + len(b) - 1) // len(b)))[:payload_size]
    if spec.startswith("hex:"):
        hx = spec[len("hex:"):].replace(" ", "")
        raw = bytes.fromhex(hx)
        return (raw * ((payload_size + len(raw) - 1) // len(raw)))[:payload_size]
    if spec.startswith("number:"):
        n = int(spec[len("number:"):].strip(), 0)
        blen = max(1, (n.bit_length() + 7) // 8)
        raw = n.to_bytes(blen, "big")
        return (raw * ((payload_size + len(raw) - 1) // len(raw)))[:payload_size]
    raise ValueError("Unknown pattern spec")

# ---------------- helpers ----------------
def build_packet(seq: int, payload: bytes) -> bytes:
    ts = time.time()
    return struct.pack(HEADER_FMT, seq, ts) + payload

def calc_pps_from_mbps(mbps: float, total_packet_size: int) -> float:
    return (mbps * 1_000_000) / (total_packet_size * 8)

# ---------------- token bucket ----------------
class TokenBucket:
    def __init__(self, rate_bps: float, burst_bytes: int):
        self.rate_bps = float(rate_bps)
        self.capacity = float(burst_bytes)
        self.tokens = float(burst_bytes)
        self.last = time.perf_counter()

    def consume(self, amount: int) -> bool:
        now = time.perf_counter()
        self.tokens = min(self.capacity, self.tokens + (now - self.last) * self.rate_bps)
        self.last = now
        if self.tokens >= amount:
            self.tokens -= amount
            return True
        return False

    def wait_time_for(self, amount: int) -> float:
        now = time.perf_counter()
        available = min(self.capacity, self.tokens + (now - self.last) * self.rate_bps)
        if available >= amount:
            return 0.0
        return (amount - available) / self.rate_bps

# ---------------- sender thread ----------------
def sender_thread(target: str, port: int, payload: bytes, tos: int,
                  rate_bytes_per_sec: float, burst_bytes: int, duration: float,
                  verify: bool, stats_dict: dict, thread_id: int):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4*1024*1024)
    if tos is not None:
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos)
        except Exception:
            pass

    tb = TokenBucket(rate_bytes_per_sec, burst_bytes)
    seq = 0
    total_sent = 0
    total_failed = 0
    rtt_list: List[float] = []
    seq_received: Set[int] = set()
    start_time = time.time()
    packet_len = HEADER_SIZE + len(payload)

    next_report = start_time + 10  # first live report after 10s

    while time.time() - start_time < duration:
        wait = tb.wait_time_for(packet_len)
        if wait > 0:
            time.sleep(wait)
            continue
        if not tb.consume(packet_len):
            time.sleep(0.00001)
            continue
        packet = struct.pack(HEADER_FMT, seq, time.time()) + payload
        try:
            sock.sendto(packet, (target, port))
            total_sent += 1
        except Exception:
            total_failed += 1

        seq = (seq + 1) & ((1 << 64) - 1)

        if verify:
            try:
                sock.settimeout(0)
                while True:
                    data, addr = sock.recvfrom(65536)
                    if not data:
                        break
                    try:
                        r_seq, ts = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
                    except Exception:
                        continue
                    rtt_list.append((time.time() - ts)*1000)
                    seq_received.add(r_seq)
            except (BlockingIOError, socket.timeout):
                pass
            except Exception:
                pass

        # live report cada 10s
        now = time.time()
        if now >= next_report:
            if rtt_list:
                latency_min = min(rtt_list)
                latency_max = max(rtt_list)
                latency_avg = statistics.mean(rtt_list)
                jitter = statistics.stdev(rtt_list) if len(rtt_list)>1 else 0.0
            else:
                latency_min = latency_max = latency_avg = jitter = 0.0
            print(f"\n[Live {int(now-start_time)}s] Sent={total_sent} PL={total_sent-len(seq_received)} "
                  f"Min={latency_min:.2f}ms Max={latency_max:.2f}ms Avg={latency_avg:.2f}ms Jitter={jitter:.2f}ms")
            next_report += 10

    sock.close()
    stats_dict[thread_id] = (total_sent, total_failed, rtt_list, seq_received)

# ---------------- top-level ----------------
def run_high_perf_sender(target_host: str, target_port: int, pps: float, mbps: float,
                         payload_size: int, pattern: str, verify: bool,
                         flows: int, sndbuf: int, tos: int, duration: float):
    total_packet_size = HEADER_SIZE + payload_size
    if mbps and pps:
        raise ValueError("Use either PPS or Mbps")
    if not (mbps or pps):
        raise ValueError("Specify PPS or Mbps")
    pps_calc = calc_pps_from_mbps(mbps, total_packet_size) if mbps else pps
    bps = pps_calc * total_packet_size
    rate_bytes_per_sec = bps
    burst_bytes = max(65536, total_packet_size*100)

    logging.info("Target PPS=%.3f, packet=%d bytes, rate=%.0f B/s, burst=%d",
                 pps_calc, total_packet_size, rate_bytes_per_sec, burst_bytes)

    payload = pattern_bytes_from_spec(payload_size, pattern)

    threads = []
    stats_dict = {}
    for i in range(flows):
        t = threading.Thread(target=sender_thread,
                             args=(target_host, target_port, payload, tos,
                                   rate_bytes_per_sec/flows, burst_bytes//flows, duration,
                                   verify, stats_dict, i))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # ---------------- aggregate statistics ----------------
    total_sent = sum(v[0] for v in stats_dict.values())
    total_failed = sum(v[1] for v in stats_dict.values())
    all_rtt = [rtt for v in stats_dict.values() for rtt in v[2]]
    all_seq_received = set()
    total_rx_bytes = 0
    for v in stats_dict.values():
        all_seq_received.update(v[3])
        total_rx_bytes += len(v[3]) * total_packet_size

    packet_loss = total_sent - len(all_seq_received)
    loss_percent = (packet_loss/total_sent*100) if total_sent else 0.0
    duration_actual = duration if duration > 0 else 1.0
    tx_mbps = (total_packet_size*8*total_sent)/duration_actual/1_000_000
    tx_pps = total_sent/duration_actual
    rx_mbps = (total_rx_bytes*8)/duration_actual/1_000_000
    rx_pps = len(all_seq_received)/duration_actual

    # ---------------- print final ----------------
    print("\nUDP Traffic Statistics (Final):")
    print(f"{'Metric':<25} {'Value'}")
    print(f"{'-'*40}")
    print(f"{'Total TX Packets':<25} {total_sent}")
    print(f"{'Total RX Packets':<25} {len(all_seq_received)}")
    print(f"{'Packet Loss':<25} {packet_loss}")
    print(f"{'Packet Loss %':<25} {loss_percent:.2f}%")
    print(f"{'TX Mbps':<25} {tx_mbps:.3f}")
    print(f"{'TX PPS':<25} {tx_pps:.0f}")
    print(f"{'RX Mbps':<25} {rx_mbps:.3f}")
    print(f"{'RX PPS':<25} {rx_pps:.0f}")
    if all_rtt:
        print(f"{'Min Latency (ms)':<25} {min(all_rtt):.3f}")
        print(f"{'Max Latency (ms)':<25} {max(all_rtt):.3f}")
        print(f"{'Mean Latency (ms)':<25} {statistics.mean(all_rtt):.3f}")
        if len(all_rtt) > 1:
            print(f"{'Std/Jitter (ms)':<25} {statistics.stdev(all_rtt):.3f}")

# ---------------- CLI ----------------
def main():
    parser = argparse.ArgumentParser(description="High-performance UDP sender Windows-friendly with live stats")
    parser.add_argument("host")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--pps", type=float, default=None)
    parser.add_argument("--mbps", type=float, default=None)
    parser.add_argument("--payload-size", type=int, default=64)
    parser.add_argument("--pattern", type=str, default="prbs")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--flows", type=int, default=1)
    parser.add_argument("--sndbuf", type=int, default=4*1024*1024)
    parser.add_argument("--tos", type=int, default=None)
    parser.add_argument("--duration", type=float, default=30, help="seconds")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    run_high_perf_sender(args.host, args.port, args.pps or 0, args.mbps or 0,
                         args.payload_size, args.pattern, args.verify,
                         args.flows, args.sndbuf, args.tos, args.duration)

if __name__ == "__main__":
    main()
