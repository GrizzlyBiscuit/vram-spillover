# Spillover Bench Results

## Proof run 1 (battery.log.run1)
- handoff: 67 tokens / 231 chars
- stream_end: 620 tokens / 2403 chars
- post-handoff: 553 tokens / 2172 chars
- elapsed: 25.8s
- fallback throughput: 32.9 tok/s
- models: Kev-4B-GGUF + qwen3.6-35b-spec-pair-q8
- finish_reason: stop

## Proof run 2 (battery2.log)
- handoff: 68 tokens / 232 chars
- stream_end: 621 tokens / 2404 chars
- post-handoff: 553 tokens / 2172 chars
- elapsed: 23.2s
- fallback throughput: 32.9 tok/s
- models: Kev-4B-GGUF + qwen3.6-35b-spec-pair-q8
- finish_reason: stop

## Control leg (direct :8081, no proxy/watcher)
- chunks: 657
- chars: 2476
- elapsed: 34.0s
- finish_reason: stop
- handoff_possible: NO
- re-prefill: 2.724s cold / 0.444s warm

## KV growth rate (live run 2026-10-26)
- tokens_emitted: 195
- samples: 186
- VRAM min: 31336 MiB
- VRAM max: 31336 MiB
- delta: 0 MiB
- slope: 0.000000 GB/1k tokens

## Static safe-settings run (direct :8081)
- max_tokens: 100
- prompt_tokens: 57
- completion_tokens: 100
- total_tokens: 157
- finish_reason: length
- predicted_ms: 5019.241
- predicted_per_token_ms: 50.70
- predicted_per_second: 19.72
- VRAM before/after: 31336 MiB (flat)
