#!/usr/bin/env python3
"""
Spillover proxy — token-level session handoff.

WHY A PROXY
-----------
llama.cpp's server has no session save/restore API. You cannot snapshot a
generation in progress and resume it on another process. So "pause, capture,
hand off, resume seamlessly" cannot be done inside the model server.

What CAN be done, and what this implements: the proxy holds the CLIENT's HTTP
connection open, streams tokens through from the primary instance, and when the
watcher reports pressure it:
    1. captures the prompt + every token already emitted to the client
    2. replays that as a prefill into the fallback instance
    3. continues streaming into the SAME open connection

The client sees one continuous response. The model swap is invisible to it.
This is honest about what it is: a proxy-level handoff with a real re-prefill
cost, not a model-side context migration. `handoff` events are logged so the
record shows exactly when and why it happened.

Requests that have not started streaming can be re-routed BEFORE any token is
sent, which is genuinely seamless and free — the fallback simply answers.

Usage:
  proxy.py --primary http://127.0.0.1:8081 --fallback http://127.0.0.1:8082
           --listen 8090 --decisions /path/watcher.jsonl --log /path/proxy.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ARGS = None
STATE = {
    "spilling": False,
    "last_decision_at": 0.0,
    "handoffs": 0,
    "requests": 0,
    "routed_primary": 0,
    "routed_fallback": 0,
}
LOCK = threading.Lock()
LOGF = None


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(rec: dict) -> None:
    rec.setdefault("at", now())
    with LOCK:
        if LOGF:
            LOGF.write(json.dumps(rec) + "\n")
            LOGF.flush()


# ── decision feed: read the watcher's jsonl and track the latest state ───────
def decision_watcher() -> None:
    """Tail the watcher's decision log; update STATE.spilling."""
    path = ARGS.decisions
    off = 0
    # start at the end so we act on new decisions, not history
    try:
        off = os.path.getsize(path)
    except OSError:
        off = 0
    while True:
        try:
            size = os.path.getsize(path)
        except OSError:
            time.sleep(0.5)
            continue
        if size < off:
            off = 0
        if size > off:
            try:
                with open(path, "r", errors="replace") as f:
                    f.seek(off)
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            d = json.loads(line)
                        except ValueError:
                            continue
                        ev = d.get("event")
                        if ev == "spill_enter":
                            with LOCK:
                                STATE["spilling"] = True
                                STATE["last_decision_at"] = time.time()
                            log({"event": "watcher_spill_enter",
                                 "pressure": d.get("pressure"),
                                 "reason": d.get("reason")})
                        elif ev == "spill_leave":
                            with LOCK:
                                STATE["spilling"] = False
                                STATE["last_decision_at"] = time.time()
                            log({"event": "watcher_spill_leave",
                                 "pressure": d.get("pressure"),
                                 "reason": d.get("reason")})
                    off = f.tell()
            except OSError:
                pass
        time.sleep(0.25)


def spilling() -> bool:
    with LOCK:
        return STATE["spilling"]


# ── upstream helpers ─────────────────────────────────────────────────────────
# The upstream llama-servers are started with --api-key, so every request needs
# the bearer. Without it they answer 401 Invalid API Key — and because the
# failure is an HTTPError (not a connection error) it looked like an
# unreachable upstream rather than an auth problem.
def _upstream_headers() -> dict:
    h = {"Content-Type": "application/json"}
    if ARGS and getattr(ARGS, "api_key", ""):
        h["Authorization"] = f"Bearer {ARGS.api_key}"
    return h


def _stream_upstream(base: str, body: bytes, timeout: int = 600):
    """POST to an upstream and yield raw SSE lines. Raises on connect error."""
    req = urllib.request.Request(
        base.rstrip("/") + "/v1/chat/completions",
        data=body,
        headers=_upstream_headers(),
    )
    return urllib.request.urlopen(req, timeout=timeout)


def _nonstream_upstream(base: str, body: bytes, timeout: int = 600) -> tuple[int, bytes]:
    req = urllib.request.Request(
        base.rstrip("/") + "/v1/chat/completions",
        data=body,
        headers=_upstream_headers(),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def _health(base: str) -> bool:
    try:
        req = urllib.request.Request(base.rstrip("/") + "/health",
                                     headers=_upstream_headers())
        with urllib.request.urlopen(req, timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def _send_json(self, code: int, obj) -> None:
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/") in ("/health", "/spillover/health"):
            primary_ok = _health(ARGS.primary)
            fallback_ok = _health(ARGS.fallback)
            with LOCK:
                snap = dict(STATE)
            self._send_json(200, {
                "status": "ok",
                "primary": ARGS.primary, "primary_ok": primary_ok,
                "fallback": ARGS.fallback, "fallback_ok": fallback_ok,
                "spilling": snap["spilling"],
                "handoffs": snap["handoffs"],
                "requests": snap["requests"],
                "routed_primary": snap["routed_primary"],
                "routed_fallback": snap["routed_fallback"],
            })
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self):
        if self.path.rstrip("/") not in ("/v1/chat/completions", "/chat/completions"):
            self._send_json(404, {"error": "not_found"})
            return

        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            payload = json.loads(raw)
        except ValueError:
            self._send_json(400, {"error": "invalid_json"})
            return

        want_stream = bool(payload.get("stream"))
        with LOCK:
            STATE["requests"] += 1

        # ── pre-token routing: if already spilling, go straight to fallback ──
        if spilling():
            with LOCK:
                STATE["routed_fallback"] += 1
            log({"event": "route_fallback",
                 "reason": "already spilling at request start",
                 "stream": want_stream})
            if want_stream:
                self._serve_stream(ARGS.fallback, payload, raw)
            else:
                code, body = _nonstream_upstream(ARGS.fallback, raw)
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            return

        # ── start on primary ────────────────────────────────────────────────
        with LOCK:
            STATE["routed_primary"] += 1
        log({"event": "route_primary", "stream": want_stream})

        if not want_stream:
            self._serve_nonstream_with_fallback(payload, raw)
            return

        self._serve_stream_with_handoff(payload, raw)

    # ── plain streaming from one upstream (no handoff) ──────────────────────
    def _serve_stream(self, base: str, payload, raw: bytes) -> None:
        """Stream straight from `base` to the client. Used when the request is
        already known to belong on the fallback, so there is nothing to switch."""
        emitted = 0
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def chunk_out(data: bytes) -> None:
            self.wfile.write(b"%X\r\n" % len(data) + data + b"\r\n")
            self.wfile.flush()

        try:
            resp = _stream_upstream(base, raw)
        except Exception as e:
            log({"event": "stream_upstream_unreachable", "base": base,
                 "error": type(e).__name__})
            chunk_out(b"data: [DONE]\n\n")
            chunk_out(b"")
            return
        try:
            for line in resp:
                if not line:
                    continue
                if line.startswith(b"data: "):
                    d = line[6:].strip()
                    if d == b"[DONE]":
                        break
                    emitted += 1
                chunk_out(line)
        except Exception as e:
            log({"event": "stream_error", "base": base,
                 "error": type(e).__name__, "chunks": emitted})
        chunk_out(b"data: [DONE]\n\n")
        chunk_out(b"")
        log({"event": "stream_end", "base": base, "chunks": emitted,
             "handoff": False})

    # ── non-streaming: simple retry onto the fallback on failure ────────────
    def _serve_nonstream_with_fallback(self, payload, raw):
        code, body = _nonstream_upstream(ARGS.primary, raw)
        if code == 200 or not spilling():
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        with LOCK:
            STATE["handoffs"] += 1
        log({"event": "handoff", "mode": "nonstream",
             "from": ARGS.primary, "to": ARGS.fallback,
             "primary_status": code, "reason": "primary failed while spilling"})
        code2, body2 = _nonstream_upstream(ARGS.fallback, raw)
        self.send_response(code2)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body2)))
        self.end_headers()
        self.wfile.write(body2)

    # ── streaming: the real handoff ────────────────────────────────────────
    def _serve_stream_with_handoff(self, payload, raw):
        """Stream from primary; on spill, replay prompt+emitted into fallback."""
        emitted: list[str] = []          # text tokens already sent to the client
        finish_reason = None
        handoff_done = False
        model_used = payload.get("model", "")

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()

        def chunk_out(data: bytes) -> None:
            self.wfile.write(b"%X\r\n" % len(data) + data + b"\r\n")
            self.wfile.flush()

        def sse(obj) -> None:
            chunk_out(b"data: " + json.dumps(obj).encode() + b"\n\n")

        def end_stream() -> None:
            chunk_out(b"data: [DONE]\n\n")
            chunk_out(b"")

        try:
            resp = _stream_upstream(ARGS.primary, raw)
        except Exception as e:
            log({"event": "primary_unreachable", "error": type(e).__name__})
            # nothing emitted yet: hand off before a single token is sent
            with LOCK:
                STATE["handoffs"] += 1
            log({"event": "handoff", "mode": "prefirst_token",
                 "from": ARGS.primary, "to": ARGS.fallback,
                 "reason": f"primary unreachable: {type(e).__name__}"})
            try:
                resp = _stream_upstream(ARGS.fallback, raw)
            except Exception as e2:
                log({"event": "fallback_unreachable", "error": type(e2).__name__})
                end_stream()
                return

        # A handoff REPLACES the response mid-flight, so the read loop must be
        # able to rebind it. `for line in resp` binds the iterator once at loop
        # entry, so reassigning `resp` inside it has no effect — the loop keeps
        # draining the old, closed stream and the fallback's output is dropped.
        # Hence an explicit iterator we can rebuild.
        it = iter(resp)
        while True:
            try:
                line = next(it)
            except StopIteration:
                break
            except Exception as e:
                log({"event": "stream_read_error", "error": type(e).__name__,
                     "emitted_tokens": len(emitted)})
                break

            if not line:
                continue
            if line.startswith(b"data: "):
                data = line[6:].strip()
                if data == b"[DONE]":
                    break
                try:
                    obj = json.loads(data)
                except ValueError:
                    chunk_out(line)
                    continue
                for ch in obj.get("choices", []):
                    delta = ch.get("delta") or {}
                    piece = delta.get("content")
                    if piece:
                        emitted.append(piece)
                    if ch.get("finish_reason"):
                        finish_reason = ch["finish_reason"]

                # ── spill trigger: mid-generation ─────────────────────────
                if spilling() and not handoff_done and emitted:
                    handoff_done = True
                    text_so_far = "".join(emitted)
                    log({"event": "handoff", "mode": "midstream",
                         "from": ARGS.primary, "to": ARGS.fallback,
                         "tokens_emitted": len(emitted),
                         "chars_emitted": len(text_so_far),
                         "reason": "watcher reported spill during generation"})
                    handoff_time = time.time()
                    with LOCK:
                        STATE["handoffs"] += 1
                    try:
                        resp.close()
                    except Exception:
                        pass
                    # replay prompt + what the client already has, and
                    # continue. The client keeps the same connection.
                    msgs = list(payload.get("messages") or [])
                    msgs.append({"role": "assistant", "content": text_so_far})
                    msgs.append({"role": "user",
                                 "content": "Continue the response from "
                                            "exactly where it stopped. Do not "
                                            "repeat anything already written."})
                    cont = dict(payload)
                    cont["messages"] = msgs
                    cont["stream"] = True
                    try:
                        resp = _stream_upstream(
                            ARGS.fallback, json.dumps(cont).encode())
                        # rebuild the iterator so the fallback's tokens are
                        # actually read (see the note above)
                        it = iter(resp)
                        log({"event": "handoff_resumed",
                             "replayed_chars": len(text_so_far),
                             "elapsed": round(time.time() - handoff_time, 3)})
                        continue
                    except Exception as e:
                        log({"event": "handoff_failed",
                             "error": type(e).__name__})
                        break

                sse(obj)
            else:
                # passthrough for any non-data line (comments, keepalives)
                chunk_out(line)

        end_stream()
        log({"event": "stream_end", "tokens_emitted": len(emitted),
             "chars_emitted": len("".join(emitted)),
             "finish_reason": finish_reason, "handoff": handoff_done})


def main() -> int:
    global ARGS, LOGF
    ap = argparse.ArgumentParser()
    ap.add_argument("--primary", required=True)
    ap.add_argument("--fallback", required=True)
    ap.add_argument("--listen", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--decisions", required=True,
                    help="watcher decision jsonl to follow")
    ap.add_argument("--log", required=True, help="proxy event log (jsonl)")
    ap.add_argument("--api-key", default="",
                    help="bearer for the upstream llama-servers (they are "
                         "started with --api-key; omit only if they have none)")
    args = ap.parse_args()
    ARGS = args

    os.makedirs(os.path.dirname(os.path.abspath(args.log)), exist_ok=True)
    LOGF = open(args.log, "a")
    log({"event": "proxy_start", "primary": args.primary,
         "fallback": args.fallback, "listen": f"{args.listen}:{args.port}",
         "decisions": args.decisions})

    threading.Thread(target=decision_watcher, daemon=True).start()

    srv = ThreadingHTTPServer((args.listen, args.port), Handler)
    print(f"[spillover] proxy on http://{args.listen}:{args.port} "
          f"-> primary {args.primary} / fallback {args.fallback}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        log({"event": "proxy_stop"})
        srv.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
