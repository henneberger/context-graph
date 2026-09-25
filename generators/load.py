#!/usr/bin/env python3
"""Standard-library load generators for the context graph ingestion API."""
import argparse
import concurrent.futures
import datetime as dt
import http.client
import json
import random
import re
import unicodedata
import os
from pathlib import Path
import ssl
import ipaddress
import subprocess
import threading
import time
import urllib.parse


def check_response(status, body, streaming=False):
    if not 200 <= status < 300:
        raise RuntimeError(f"HTTP {status}: {body[:500]}")
    records = [json.loads(line) for line in body.splitlines() if line.strip()]
    if not records:
        raise RuntimeError("Empty response")
    for record in records:
        if record.get("status") == "error" or "error" in record:
            raise RuntimeError(f"Ingestion error: {record}")
    if streaming and records[-1].get("status") != "complete":
        raise RuntimeError("Video response ended without completion acknowledgement")
    return records[-1]


def connection(base, timeout, ca=None, allow_http=False):
    url = urllib.parse.urlsplit(base)
    if url.scheme not in ("http", "https") or not url.hostname:
        raise ValueError("URL must be http:// or https://")
    if url.scheme == "https":
        context = ssl.create_default_context(cafile=ca)
        return http.client.HTTPSConnection(url.hostname, url.port, timeout=timeout, context=context), url.path.rstrip("/")
    try:
        loopback = ipaddress.ip_address(url.hostname).is_loopback
    except ValueError:
        loopback = url.hostname == "localhost"
    if not loopback and not allow_http:
        raise ValueError("Remote HTTP requires --allow-http; prefer verified HTTPS")
    return http.client.HTTPConnection(url.hostname, url.port, timeout=timeout), url.path.rstrip("/")


def auth_headers(args):
    token = args.token
    if args.token_file:
        token = Path(args.token_file).read_text().strip()
        if token.startswith("{"):
            token = json.loads(token).get("access_token")
    if not isinstance(token, str) or not token.strip() or "\r" in token or "\n" in token:
        raise ValueError("A nonempty token or token response file is required")
    if not token.startswith("Bearer "):
        token = "Bearer " + token
    return {"Authorization": token, "X-Workspace-Id": args.workspace}


def entity_id(args, sequence):
    if args.entity:
        return args.entity[sequence % len(args.entity)]
    return f"{args.entity_prefix}{sequence % args.entities:04d}"


def upload(base, path, body, content_type, entity=None, timeout=90, *, headers=None, ca=None, allow_http=False):
    if not headers or not headers.get("Authorization") or not headers.get("X-Workspace-Id"):
        raise ValueError("Authenticated workspace headers are required")
    conn, prefix = connection(base, timeout, ca, allow_http)
    try:
        headers = dict(headers, **{"Content-Type": content_type})
        if entity:
            headers["X-Entity-Id"] = entity
        conn.request("POST", prefix + path, body=body, headers=headers)
        response = conn.getresponse()
        return check_response(response.status, response.read(2 * 1024 * 1024).decode())
    finally:
        conn.close()


def json_payload(args, sequence, rng):
    entity = entity_id(args, sequence)
    timestamp = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=rng.uniform(0, args.out_of_order_seconds))
    value = "intentionally-invalid" if rng.random() < args.invalid_ratio else round(20 + rng.gauss(0, 3), 3)
    payload = {"entityId": entity, "eventTime": timestamp.isoformat().replace("+00:00", "Z"),
               "metric": args.metric, "value": value}
    if args.related_to:
        payload["relatedTo"] = args.related_to
    if args.schema_drift:
        payload["extra"] = {"revision": sequence // args.entities, "tags": ["generator", args.metric], "nested": {"active": True}}
        payload[f"dynamic_{sequence % 10}"] = sequence
    return payload


def image_bytes(args):
    if args.file:
        with open(args.file, "rb") as source:
            return source.read()
    command = ["ffmpeg", "-v", "error", "-f", "lavfi", "-i", f"testsrc2=size={args.width}x{args.height}:rate=1",
               "-frames:v", "1", "-threads", "1", "-f", "image2pipe", "-c:v", "png", "pipe:1"]
    return subprocess.run(command, stdout=subprocess.PIPE, check=True, timeout=30).stdout


def video_command(args):
    command = ["ffmpeg", "-hide_banner", "-loglevel", "error"]
    if not args.no_realtime:
        command += ["-re"]
    if args.file:
        command += ["-stream_loop", "-1", "-i", args.file]
    else:
        command += ["-f", "lavfi", "-i", f"testsrc2=size={args.width}x{args.height}:rate={args.fps}"]
    command += ["-t", str(args.stream_seconds), "-an", "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
                "-threads", "1", "-pix_fmt", "yuv420p", "-g", str(args.gop), "-keyint_min", str(args.gop), "-sc_threshold", "0"]
    if args.keyframe_seconds:
        command += ["-force_key_frames", f"expr:gte(t,n_forced*{args.keyframe_seconds})"]
    return command + ["-f", "mpegts", "pipe:1"]


def stream_video(args, sequence):
    """Read the response concurrently: the server sends headers before upload EOF."""
    conn, prefix = connection(args.url, args.stream_seconds + 90, args.ca, args.allow_http)
    process = None
    reader = None
    responses = []
    failures = []
    try:
        conn.putrequest("POST", prefix + args.path)
        conn.putheader("Content-Type", "video/mp2t")
        conn.putheader("Transfer-Encoding", "chunked")
        conn.putheader("X-Entity-Id", entity_id(args, sequence))
        for name, value in auth_headers(args).items():
            conn.putheader(name, value)
        conn.endheaders()

        def read_response():
            try:
                response = conn.getresponse()
                # Drain throughout upload, retaining only bounded response data.
                lines, total = [], 0
                while line := response.readline(65537):
                    total += len(line)
                    if total > 2 * 1024 * 1024 or len(line) > 65536:
                        raise RuntimeError("Oversized ingestion response")
                    lines.append(line.decode())
                responses.append(check_response(response.status, "".join(lines), streaming=True))
            except Exception as error:
                failures.append(error)

        reader = threading.Thread(target=read_response, daemon=True)
        reader.start()
        process = subprocess.Popen(video_command(args), stdout=subprocess.PIPE)
        while data := process.stdout.read(32 * 1024):
            if failures:
                raise failures[0]
            conn.send(f"{len(data):x}\r\n".encode() + data + b"\r\n")
        if process.wait(timeout=30) != 0:
            raise RuntimeError("FFmpeg generator failed")
        conn.send(b"0\r\n\r\n")
        reader.join(args.stream_seconds + 90)
        if reader.is_alive():
            raise TimeoutError("Timed out waiting for video completion")
        if failures:
            raise failures[0]
        if not responses:
            raise RuntimeError("No video acknowledgement")
        return responses[0]
    finally:
        if process:
            if process.poll() is None:
                process.kill()
            process.wait()
            process.stdout.close()
        conn.close()
        if reader:
            reader.join(2)


def run(args):
    auth_headers(args)  # Fail before generating bytes when credentials are absent or malformed.
    if args.mode == "images":
        content = image_bytes(args)
    start = time.monotonic()
    stop = start + args.duration
    stats = {"submitted": 0, "succeeded": 0, "failed": 0, "latency_seconds_sum": 0.0}
    lock = threading.Lock()
    slots = threading.Semaphore(args.concurrency)
    errors = []

    def execute(sequence):
        before = time.monotonic()
        try:
            if args.mode == "json":
                payload = json_payload(args, sequence, random.Random(args.seed + sequence))
                upload(args.url, args.path, json.dumps(payload).encode(), "application/json", headers={**auth_headers(args),"X-Resource-Key":payload["entityId"]}, ca=args.ca, allow_http=args.allow_http)
            elif args.mode == "images":
                upload(args.url, args.path, content, "application/octet-stream", entity_id(args, sequence), headers=auth_headers(args), ca=args.ca, allow_http=args.allow_http)
            else:
                stream_video(args, sequence)
            with lock:
                stats["succeeded"] += 1
        except Exception as error:
            with lock:
                stats["failed"] += 1
                if len(errors) < 10:
                    errors.append(str(error))
        finally:
            with lock:
                stats["latency_seconds_sum"] += time.monotonic() - before
            slots.release()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        while time.monotonic() < stop:
            target = start + stats["submitted"] / args.rate
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(min(delay, max(0, stop - time.monotonic())))
            if time.monotonic() >= stop or not slots.acquire(timeout=max(0, stop - time.monotonic())):
                break
            sequence = stats["submitted"]
            stats["submitted"] += 1
            pool.submit(execute, sequence)
    stats["elapsed_seconds"] = round(time.monotonic() - start, 3)
    stats["requests_per_second"] = round(stats["submitted"] / max(stats["elapsed_seconds"], 0.001), 3)
    stats["mean_latency_seconds"] = round(stats.pop("latency_seconds_sum") / max(stats["submitted"], 1), 3)
    stats["errors"] = errors
    print(json.dumps(stats, indent=2))
    return 1 if stats["failed"] else 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["json", "images", "video"])
    parser.add_argument("--url", default="http://localhost:18088")
    credentials = parser.add_mutually_exclusive_group(required=True)
    credentials.add_argument("--token-file", help="Private raw JWT or token-response JSON; reread before each request")
    credentials.add_argument("--token", help="Bearer JWT (prefer --token-file to avoid process argument exposure)")
    parser.add_argument("--workspace", required=True)
    identities = parser.add_mutually_exclusive_group(required=True)
    identities.add_argument("--entity", action="append", help="Pre-provisioned entity ID; repeat to cycle a known set")
    identities.add_argument("--entity-prefix", help="Explicit pre-provisioned prefix; cycles --entities suffixes 0000..")
    parser.add_argument("--related-to", help="Pre-provisioned target entity for graph edges")
    parser.add_argument("--ca", default=str(Path('.runtime/security/ca.crt')) if Path('.runtime/security/ca.crt').exists() else None, help="CA bundle for verified HTTPS")
    parser.add_argument("--allow-http", action="store_true", help="Explicitly allow cleartext HTTP beyond loopback development")
    parser.add_argument("--path", help="Override configured ingestion endpoint")
    parser.add_argument("--rate", type=float, default=10, help="Total request starts per second (video: stream starts)")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--duration", type=float, default=30, help="Scheduling duration; in-flight uploads then finish")
    parser.add_argument("--entities", type=int, default=20)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--metric", default="temperature")
    parser.add_argument("--schema-drift", action="store_true")
    parser.add_argument("--invalid-ratio", type=float, default=0)
    parser.add_argument("--out-of-order-seconds", type=float, default=0)
    parser.add_argument("--file", help="Image file or repeating video source")
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=360)
    parser.add_argument("--fps", type=int, default=30)
    parser.add_argument("--gop", type=int, default=97, help="Source GOP, deliberately non-aligned with 2-second output")
    parser.add_argument("--keyframe-seconds", type=float)
    parser.add_argument("--stream-seconds", type=float, default=30)
    parser.add_argument("--no-realtime", action="store_true", help="Generate video as fast as possible")
    args = parser.parse_args(argv)
    for name in ("rate", "concurrency", "duration", "entities", "width", "height", "fps", "gop", "stream_seconds"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if not 0 <= args.invalid_ratio <= 1 or args.out_of_order_seconds < 0:
        parser.error("invalid-ratio must be in [0,1] and out-of-order-seconds must be nonnegative")
    if args.keyframe_seconds is not None and args.keyframe_seconds <= 0:
        parser.error("keyframe-seconds must be positive")
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', args.workspace):
        parser.error("--workspace must be a valid workspace identifier")
    for entity in (args.entity or [args.entity_prefix]) + ([args.related_to] if args.related_to else []):
        if not entity or not entity.strip() or len(entity.encode("utf-16-le"))//2>256 or any(unicodedata.category(c)=="Cc" for c in entity):
            parser.error("Entity identifiers must be nonempty, at most 256 characters, with no controls")
    if args.entity_prefix and len(args.entity_prefix.encode("utf-16-le"))//2+max(4,len(str(args.entities-1)))>256:
        parser.error("Generated entity IDs exceed the 256-character bound")
    if args.entity:
        args.entities = len(args.entity)
    args.path = args.path or {"json": "/ingest/events", "images": "/ingest/images", "video": "/ingest/video"}[args.mode]
    return args


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
