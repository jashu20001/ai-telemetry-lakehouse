import argparse, json, os, re, time, uuid
from datetime import datetime, timezone
from typing import Any, Dict, List
import pandas as pd
from pydantic import BaseModel, Field, ValidationError
from kafka import KafkaConsumer

# --------- simple helpers ---------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def redact(text: str) -> str:
    if not isinstance(text, str):
        return text
    # emails
    text = re.sub(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[REDACTED_EMAIL]", text)
    # long digit sequences (potential phone/ids)
    text = re.sub(r"\d{5,}", "[REDACTED_NUMBER]", text)
    return text

# --------- data contracts (Pydantic) ---------
class PromptEvt(BaseModel):
    event_type: str = Field(pattern="prompt")
    ts: str
    prompt_id: str
    user_id: str
    team: str
    model: str
    prompt: str
    prompt_hash: str

class ResponseEvt(BaseModel):
    event_type: str = Field(pattern="response")
    ts: str
    prompt_id: str
    user_id: str
    team: str
    model: str
    latency_ms: int
    tokens_in: int
    tokens_out: int
    total_cost_usd: float
    http_status: int
    flags: Dict[str, Any]

# --------- IO ---------
def write_parquet(df: pd.DataFrame, path: str, endpoint: str, key: str, secret: str):
    opts = {
        "key": key,
        "secret": secret,
        "client_kwargs": {"endpoint_url": endpoint},
    }
    df.to_parquet(path, index=False, engine="pyarrow", storage_options=opts)

def flush(buff: List[Dict[str, Any]], kind: str, bucket: str, endpoint: str, key: str, secret: str):
    if not buff:
        return
    ts = now_utc()
    dd = ts.strftime("%Y-%m-%d")
    hh = ts.strftime("%H")
    fn = f"batch_{int(ts.timestamp())}_{uuid.uuid4().hex[:8]}.parquet"
    path = f"s3://{bucket}/bronze/{kind}/date={dd}/hour={hh}/{fn}"
    df = pd.DataFrame(buff)
    write_parquet(df, path, endpoint, key, secret)
    print(f"[flush] wrote {len(buff)} {kind} records -> {path}")
    buff.clear()

def main():
    ap = argparse.ArgumentParser(description="Ingest LLM telemetry from Kafka to MinIO (Bronze Parquet)")
    ap.add_argument("--brokers", default="localhost:9094", help="Kafka bootstrap servers (use 9094 outside listener)")
    ap.add_argument("--prompts_topic", default="llm.prompts")
    ap.add_argument("--responses_topic", default="llm.responses")
    ap.add_argument("--bucket", default="llm-pulse")
    ap.add_argument("--endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    ap.add_argument("--key", default=os.environ.get("MINIO_KEY", "minioadmin"))
    ap.add_argument("--secret", default=os.environ.get("MINIO_SECRET", "minioadmin"))
    ap.add_argument("--batch_size", type=int, default=50)
    ap.add_argument("--flush_secs", type=int, default=15)
    args = ap.parse_args()

    consumer = KafkaConsumer(
        args.prompts_topic,
        args.responses_topic,
        bootstrap_servers=args.brokers,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="llm-pulse-ingest",
        consumer_timeout_ms=1000,
    )

    prompts_buf: List[Dict[str, Any]] = []
    responses_buf: List[Dict[str, Any]] = []
    last_flush = time.time()

    print(f"[ingest] connected to {args.brokers}; writing to s3://{args.bucket} (endpoint {args.endpoint})")
    try:
        while True:
            got = False
            for msg in consumer:
                got = True
                evt = msg.value
                try:
                    if msg.topic == args.prompts_topic:
                        evt["prompt"] = redact(evt.get("prompt", ""))
                        PromptEvt(**evt)  # validate
                        prompts_buf.append(evt)
                    else:
                        ResponseEvt(**evt)
                        responses_buf.append(evt)
                except ValidationError as e:
                    print(f"[warn] schema validation failed on topic {msg.topic}: {e.errors()}")
                    continue

                if len(prompts_buf) >= args.batch_size:
                    flush(prompts_buf, "prompts", args.bucket, args.endpoint, args.key, args.secret)
                if len(responses_buf) >= args.batch_size:
                    flush(responses_buf, "responses", args.bucket, args.endpoint, args.key, args.secret)

                # time-based flush
                if time.time() - last_flush >= args.flush_secs:
                    flush(prompts_buf, "prompts", args.bucket, args.endpoint, args.key, args.secret)
                    flush(responses_buf, "responses", args.bucket, args.endpoint, args.key, args.secret)
                    last_flush = time.time()

            # if no messages this loop, still do periodic flush
            if not got and time.time() - last_flush >= args.flush_secs:
                flush(prompts_buf, "prompts", args.bucket, args.endpoint, args.key, args.secret)
                flush(responses_buf, "responses", args.bucket, args.endpoint, args.key, args.secret)
                last_flush = time.time()
    except KeyboardInterrupt:
        print("[ingest] stopping…")
    finally:
        flush(prompts_buf, "prompts", args.bucket, args.endpoint, args.key, args.secret)
        flush(responses_buf, "responses", args.bucket, args.endpoint, args.key, args.secret)
        consumer.close()

if __name__ == "__main__":
    main()

