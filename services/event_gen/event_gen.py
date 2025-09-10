import argparse, json, random, time, uuid, hashlib
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

def js(x):  # serialize to JSON bytes
    return json.dumps(x, ensure_ascii=False).encode("utf-8")

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def prompt_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]

def make_producer(bootstrap="localhost:9092"):
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=js,
        acks="all",
        linger_ms=10,
        retries=5,
    )

def gen_pair(fake: Faker):
    team = random.choice(["growth", "platform", "research"])
    model = random.choice(["gpt-4.1", "gpt-4o-mini", "llama3.1-70b", "mistral-large"])
    user_id = fake.uuid4()
    prompt_text = fake.sentence(nb_words=random.randint(6, 18))
    p_id = str(uuid.uuid4())

    # --- prompt event ---
    prompt_evt = {
        "event_type": "prompt",
        "ts": now_iso(),
        "prompt_id": p_id,
        "user_id": user_id,
        "team": team,
        "model": model,
        "prompt": prompt_text,
        "prompt_hash": prompt_hash(prompt_text),
    }

    # --- response event (synthetic metrics) ---
    tok_in = int(len(prompt_text.split()) * 1.3)
    tok_out = random.randint(10, 220)
    latency_ms = random.randint(120, 4200)
    http_status = random.choices([200, 429, 500], weights=[92, 5, 3])[0]
    price_per_1k = random.choice([0.003, 0.01, 0.02])
    cost = round((tok_in + tok_out) / 1000 * price_per_1k, 5)

    flags = {
        "toxicity": random.random() < 0.02,
        "prompt_injection": random.random() < 0.01,
        "pii_leak": random.random() < 0.01,
    }

    resp_evt = {
        "event_type": "response",
        "ts": now_iso(),
        "prompt_id": p_id,
        "user_id": user_id,
        "team": team,
        "model": model,
        "latency_ms": latency_ms,
        "tokens_in": tok_in,
        "tokens_out": tok_out,
        "total_cost_usd": cost,
        "http_status": http_status,
        "flags": flags,
    }

    return prompt_evt, resp_evt

def main():
    ap = argparse.ArgumentParser(description="LLM telemetry event generator")
    ap.add_argument("--eps", type=float, default=3.0, help="events per second (pairs/sec)")
    ap.add_argument("--seconds", type=int, default=60, help="how long to run")
    ap.add_argument("--brokers", default="localhost:9092", help="Kafka bootstrap servers")
    ap.add_argument("--prompts_topic", default="llm.prompts")
    ap.add_argument("--responses_topic", default="llm.responses")
    args = ap.parse_args()

    fake = Faker()
    prod = make_producer(args.brokers)

    period = 1.0 / max(args.eps, 0.1)
    end = time.time() + args.seconds
    sent = 0

    try:
        while time.time() < end:
            p, r = gen_pair(fake)
            prod.send(args.prompts_topic, p)
            prod.send(args.responses_topic, r)
            sent += 1
            if sent % 10 == 0:
                print(f"[gen] sent {sent} pairs…")
            time.sleep(period)
    finally:
        prod.flush()
        prod.close()
        print(f"[gen] done. total pairs sent: {sent}")

if __name__ == "__main__":
    main()

