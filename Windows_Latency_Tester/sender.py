#!/usr/bin/env python3
"""
sender.py
UDP traffic generator for Windows with:
 - single socket send/recv
 - global sequence numbers (unique across flows)
 - out-of-order detection
 - two jitter metrics: StdDev and RFC3550 smoothed jitter
 - final summary + latency, cumulative PL, and jitter graphs
"""

import argparse
import socket
import struct
import time
import logging
import threading
import statistics
from typing import List, Set

import matplotlib.pyplot as plt

HEADER_FMT = "!Qd"  # seq (uint64), timestamp (double)
HEADER_SIZE = struct.calcsize(HEADER_FMT)

# ---------------- helpers ----------------
def pattern_bytes_from_spec(payload_size: int, spec: str) -> bytes:
    spec = spec.strip()
    if spec == "zeros":
        return bytes(payload_size)
    if spec == "ones":
        return bytes([0xFF]) * payload_size
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
        import os
        return os.urandom(payload_size)
    if spec.startswith("ascii:"):
        txt = spec[len("ascii:"):]
        b = txt.encode("utf-8")
        return (b * ((payload_size + len(b) - 1) // len(b)))[:payload_size]
    if spec.startswith("hex:"):
        hx = spec[len("hex:"):].replace(" ", "")
        raw = bytes.fromhex(hx)
        return (raw * ((payload_size + len(raw) - 1) // len(raw)))[:payload_size]
    raise ValueError("Unknown pattern spec")

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

# ---------------- global seq generator ----------------
class SeqGenerator:
    def __init__(self, start: int = 0):
        self._lock = threading.Lock()
        self._val = start

    def next(self) -> int:
        with self._lock:
            v = self._val
            self._val = (self._val + 1) & ((1 << 64) - 1)
            return v

# ---------------- sender thread ----------------
def sender_thread(sock: socket.socket, target: str, port: int, payload: bytes,
                  rate_bytes_per_sec: float, burst_bytes: int, duration_send: float,
                  seq_gen: SeqGenerator, sent_list: List[int], sent_lock: threading.Lock,
                  stats_lock: threading.Lock, thread_id: int, verbose: bool):
    tb = TokenBucket(rate_bytes_per_sec, burst_bytes)
    total_sent = 0
    total_failed = 0
    start_time = time.time()
    packet_len = HEADER_SIZE + len(payload)
    next_report = start_time + 10

    while time.time() - start_time < duration_send:
        wait = tb.wait_time_for(packet_len)
        if wait > 0:
            time.sleep(wait)
            continue
        if not tb.consume(packet_len):
            time.sleep(0.00001)
            continue

        seq = seq_gen.next()
        ts = time.time()
        packet = struct.pack(HEADER_FMT, seq, ts) + payload
        try:
            sock.sendto(packet, (target, port))
            total_sent += 1
            with sent_lock:
                sent_list.append(seq)
        except Exception:
            total_failed += 1

        now = time.time()
        if now >= next_report:
            if verbose:
                print(f"[Live Sender {thread_id} {int(now-start_time)}s] Sent={total_sent} Failed={total_failed}")
            next_report += 10

    with stats_lock:
        stats[('send', thread_id)] = (total_sent, total_failed)

# ---------------- receiver thread ----------------
def receiver_thread(sock: socket.socket, duration_recv: float,
                    rtt_list: List[float], rtt_lock: threading.Lock,
                    seq_received: Set[int], recv_lock: threading.Lock,
                    ooo_counter: dict, rfc_jitter_holder: dict, verbose: bool):
    start_time = time.time()
    last_seq_seen = None
    prev_transit = None
    J = 0.0
    instantaneous_jitter_list: List[float] = []

    while time.time() - start_time < duration_recv:
        try:
            data, addr = sock.recvfrom(65536)
            if not data or len(data) < HEADER_SIZE:
                continue
            recv_ts = time.time()
            try:
                seq_recv, send_ts = struct.unpack(HEADER_FMT, data[:HEADER_SIZE])
            except Exception:
                continue

            rtt_ms = (recv_ts - send_ts) * 1000.0
            with rtt_lock:
                rtt_list.append(rtt_ms)

            with recv_lock:
                seq_received.add(seq_recv)

            if last_seq_seen is None:
                last_seq_seen = seq_recv
            else:
                if seq_recv < last_seq_seen:
                    ooo_counter['count'] += 1
                    if verbose and ooo_counter['count'] <= 5:
                        print(f"[OOO] packet seq {seq_recv} arrived after {last_seq_seen}")
                else:
                    last_seq_seen = seq_recv

            transit = recv_ts - send_ts
            if prev_transit is None:
                prev_transit = transit
            else:
                D = transit - prev_transit
                J += (abs(D) - J) / 16.0
                prev_transit = transit
                instantaneous_jitter_list.append(abs(D) * 1000.0)

            rfc_jitter_holder['jitter'] = J

        except (BlockingIOError, socket.timeout):
            time.sleep(0.001)
        except Exception:
            time.sleep(0.001)

    # store instantaneous jitter for plotting
    rfc_jitter_holder['instantaneous'] = instantaneous_jitter_list

# ---------------- top-level ----------------
def run_high_perf_sender(target_host: str, target_port: int, pps: float, mbps: float,
                         payload_size: int, pattern: str, flows: int,
                         sndbuf: int, tos: int, duration: float, extra: float, local_port: int, verbose: bool):
    total_packet_size = HEADER_SIZE + payload_size
    if mbps and pps:
        raise ValueError("Use either PPS or Mbps")
    if not (mbps or pps):
        raise ValueError("Specify PPS or Mbps")

    pps_calc = calc_pps_from_mbps(mbps, total_packet_size) if mbps else pps
    bps = pps_calc * total_packet_size
    rate_bytes_per_sec = bps
    burst_bytes = max(65536, total_packet_size * 100)

    logging.info("Target PPS=%.3f, packet payload+hdr=%d bytes, rate=%.0f B/s, burst=%d",
                 pps_calc, total_packet_size, rate_bytes_per_sec, burst_bytes)

    payload = pattern_bytes_from_spec(payload_size, pattern)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, sndbuf)
    except Exception:
        pass
    if tos is not None:
        try:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, tos)
        except Exception:
            pass
    if local_port:
        sock.bind(("", local_port))

    seq_gen = SeqGenerator(0)
    sent_list: List[int] = []
    sent_lock = threading.Lock()
    seq_received: Set[int] = set()
    recv_lock = threading.Lock()
    rtt_list: List[float] = []
    rtt_lock = threading.Lock()
    stats_lock = threading.Lock()
    global stats
    stats = {}
    ooo_counter = {'count': 0}
    rfc_jitter_holder = {'jitter': 0.0}

    recv_thread = threading.Thread(target=receiver_thread,
                                   args=(sock, duration + extra, rtt_list, rtt_lock,
                                         seq_received, recv_lock, ooo_counter, rfc_jitter_holder, verbose))
    recv_thread.daemon = True
    recv_thread.start()

    send_threads = []
    for i in range(flows):
        t = threading.Thread(target=sender_thread,
                             args=(sock, target_host, target_port, payload,
                                   rate_bytes_per_sec / flows, burst_bytes // flows,
                                   duration, seq_gen, sent_list, sent_lock, stats_lock, i, verbose))
        t.daemon = False
        t.start()
        send_threads.append(t)

    for t in send_threads:
        t.join()

    recv_thread.join()
    sock.close()

    total_sent = 0
    total_failed = 0
    with stats_lock:
        for k, v in stats.items():
            if isinstance(k, tuple) and k[0] == 'send':
                total_sent += v[0]
                total_failed += v[1]

    all_sent_seq = list(sent_list)
    all_sent_set = set(all_sent_seq)
    with recv_lock:
        all_recv_set = set(seq_received)
    with rtt_lock:
        all_rtts = list(rtt_list)

    packet_loss = len(all_sent_set - all_recv_set)
    loss_percent = (packet_loss / len(all_sent_set) * 100.0) if all_sent_set else 0.0
    duration_actual_send = duration if duration > 0 else 1.0
    duration_actual_recv = duration + extra if (duration + extra) > 0 else 1.0

    tx_mbps = (total_packet_size * 8 * total_sent) / duration_actual_send / 1_000_000
    tx_pps = total_sent / duration_actual_send
    rx_mbps = (total_packet_size * 8 * len(all_recv_set)) / duration_actual_recv / 1_000_000
    rx_pps = len(all_recv_set) / duration_actual_recv

    std_jitter_ms = statistics.stdev(all_rtts) if len(all_rtts) > 1 else 0.0
    rfc_jitter_s = rfc_jitter_holder.get('jitter', 0.0)
    rfc_jitter_ms = rfc_jitter_s * 1000.0
    inst_jitter = rfc_jitter_holder.get('instantaneous', [])

    print("\nUDP Traffic Statistics (Final):")
    print(f"{'Metric':<25} {'Value'}")
    print(f"{'-'*48}")
    print(f"{'Total TX Packets':<25} {total_sent}")
    print(f"{'Total RX Packets':<25} {len(all_recv_set)}")
    print(f"{'Packet Loss':<25} {packet_loss}")
    print(f"{'Packet Loss %':<25} {loss_percent:.2f}%")
    print(f"{'Out-of-order pkts':<25} {ooo_counter['count']}")
    print(f"{'TX Mbps':<25} {tx_mbps:.3f}")
    print(f"{'TX PPS':<25} {tx_pps:.0f}")
    print(f"{'RX Mbps':<25} {rx_mbps:.3f}")
    print(f"{'RX PPS':<25} {rx_pps:.0f}")
    print(f"{'StdDev Jitter (ms)':<25} {std_jitter_ms:.3f}")
    print(f"{'RFC3550 Jitter (ms)':<25} {rfc_jitter_ms:.3f}")
    if all_rtts:
        print(f"{'Min Latency (ms)':<25} {min(all_rtts):.3f}")
        print(f"{'Max Latency (ms)':<25} {max(all_rtts):.3f}")
        print(f"{'Mean Latency (ms)':<25} {statistics.mean(all_rtts):.3f}")

    # ---------------- updated subplot layout ----------------
    fig, axs = plt.subplots(2, 2, figsize=(14,8))

    # Column 1
    axs[0,0].plot(all_rtts, marker='.', linestyle='-', linewidth=0.8, label='RTT ms')
    axs[0,0].set_title("Packet Latency (ms)")
    axs[0,0].set_xlabel("Received packet index")
    axs[0,0].set_ylabel("RTT (ms)")
    axs[0,0].grid(True)
    axs[0,0].legend()

    axs[1,0].plot([0]*len(all_sent_seq), marker='.', linestyle='-', linewidth=0.8)  # dummy x
    pl_cumulative = []
    lost = 0
    for s in all_sent_seq:
        if s not in all_recv_set:
            lost += 1
        pl_cumulative.append(lost)
    axs[1,0].plot(pl_cumulative, marker='.', linestyle='-', color='red', linewidth=0.8, label='Cumulative loss')
    axs[1,0].set_title("Cumulative Packet Loss")
    axs[1,0].set_xlabel("Sent packet index")
    axs[1,0].set_ylabel("Cumulative lost packets")
    axs[1,0].grid(True)
    axs[1,0].legend()

    # Column 2
    axs[0,1].plot(inst_jitter, linestyle='-', linewidth=0.8, color='orange', label='Instantaneous jitter')
    axs[0,1].plot([x*1000.0 for x in [rfc_jitter_s]*len(inst_jitter)], linestyle='-', linewidth=0.8, color='blue', label='RFC3550 smoothed jitter')
    axs[0,1].set_title("Jitter (ms)")
    axs[0,1].set_xlabel("Packet index (received)")
    axs[0,1].set_ylabel("Jitter (ms)")
    axs[0,1].grid(True)
    axs[0,1].legend()

    # empty placeholder for future subplot
    axs[1,1].axis('off')

    plt.tight_layout()
    plt.show()

# ---------------- CLI ----------------
def main():
    parser = argparse.ArgumentParser(description="High-performance UDP sender Windows-friendly")
    parser.add_argument("host")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--local-port", type=int, default=0, help="local source port to bind (0 = auto)")
    parser.add_argument("--pps", type=float, default=None)
    parser.add_argument("--mbps", type=float, default=None)
    parser.add_argument("--payload-size", type=int, default=64)
    parser.add_argument("--pattern", type=str, default="prbs")
    parser.add_argument("--flows", type=int, default=1)
    parser.add_argument("--sndbuf", type=int, default=4*1024*1024)
    parser.add_argument("--tos", type=int, default=None)
    parser.add_argument("--duration", type=float, default=30.0, help="sending duration (seconds)")
    parser.add_argument("--extra", type=float, default=5.0, help="extra seconds to keep receiver listening after send ends")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    run_high_perf_sender(args.host, args.port, args.pps or 0.0, args.mbps or 0.0,
                         args.payload_size, args.pattern, args.flows,
                         args.sndbuf, args.tos, args.duration, args.extra, args.local_port, args.verbose)

if __name__ == "__main__":
    main()
