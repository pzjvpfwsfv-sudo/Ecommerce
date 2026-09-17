import argparse
from datetime import UTC, datetime
import json
import math
from pathlib import Path
import re
import sys
import time

from .replay_source import read_line, skip_confirmed, source_stamp, validate_event, verify_source
from .replay_state import checkpoint_lock, load_checkpoint, save_checkpoint


def validate_topic(topic):
    if not isinstance(topic, str) or not re.fullmatch(r"real_behavior_events_v1(?:_[a-z0-9][a-z0-9_-]{0,63})?", topic):
        raise ValueError("topic must use the real_behavior_events_v1 namespace")


class DryRunSink:
    def send(self, message, key):
        pass

    def close(self):
        pass


def replay_file(input_path, checkpoint_path, *, mode="dry-run", bootstrap_servers="localhost:9092",
                topic="real_behavior_events_v1", rate=100, max_events=1000, checkpoint_every=100,
                sink_factory=None, clock=time.monotonic, sleep=time.sleep):
    validate_topic(topic)
    if (mode not in {"dry-run", "kafka"} or not isinstance(rate, (int, float))
            or not math.isfinite(rate) or rate <= 0
            or any(type(value) is not int or value < 1 for value in (max_events, checkpoint_every))
            or not isinstance(bootstrap_servers, str) or not bootstrap_servers.strip()):
        raise ValueError("invalid replay mode, rate, limits or destination")
    bootstrap_servers = bootstrap_servers.strip()
    input_path, checkpoint_path = Path(input_path).resolve(), Path(checkpoint_path).resolve()
    manifest_path = Path(str(input_path) + ".provenance.json")
    if checkpoint_path in {input_path, manifest_path} or Path(str(checkpoint_path) + ".lock").resolve() in {input_path, manifest_path}:
        raise ValueError("source, manifest and checkpoint paths must differ")
    with checkpoint_lock(checkpoint_path), input_path.open("rb") as stream:
        manifest, initial_stamp = verify_source(stream, manifest_path)
        identity = {key: manifest[key] for key in ("dataset_id", "sha256", "artifact_bytes", "sample_rows")}
        identity.update(mode=mode, bootstrap_servers=bootstrap_servers, topic=topic)
        state = load_checkpoint(checkpoint_path, identity)
        skip_confirmed(stream, manifest, state)
        if not checkpoint_path.exists():
            save_checkpoint(checkpoint_path, state)
        starting = state["confirmed_records"]
        status = "limit_reached"
        sink = None
        try:
            if starting < manifest["sample_rows"]:
                if sink_factory is not None:
                    sink = sink_factory()
                elif mode == "dry-run":
                    sink = DryRunSink()
                else:
                    from .replay_kafka import ReplayKafkaSink
                    sink = ReplayKafkaSink(bootstrap_servers, topic)
            next_send = clock()
            while state["confirmed_records"] < min(starting + max_events, manifest["sample_rows"]):
                if source_stamp(stream) != initial_stamp:
                    raise ValueError("source changed during replay")
                line = read_line(stream)
                if not line:
                    raise ValueError("source ended before its declared record count")
                event = validate_event(line, manifest)
                delay = next_send - clock()
                if delay > 0:
                    sleep(delay)
                message = event | {"replay_schema_version": 1, "replay_id": state["replay_id"], "replayed_at": datetime.now(UTC).isoformat()}
                sink.send(message, event["dataset_id"] + ":" + event["user_id"])
                # An interrupt must not observe the new count with the old event ID.
                state = state | {"confirmed_records": state["confirmed_records"] + 1, "last_event_id": event["event_id"]}
                next_send = clock() + 1 / rate
                if state["confirmed_records"] % checkpoint_every == 0:
                    save_checkpoint(checkpoint_path, state)
            if state["confirmed_records"] == manifest["sample_rows"]:
                if read_line(stream):
                    raise ValueError("source contains more records than its manifest")
                status = "complete"
            if source_stamp(stream) != initial_stamp:
                raise ValueError("source changed during replay")
            save_checkpoint(checkpoint_path, state)
        except KeyboardInterrupt:
            save_checkpoint(checkpoint_path, state)
            status = "paused"
        finally:
            if sink is not None:
                sink.close()
        return {"status": status, "mode": mode, "topic": topic, "replay_id": state["replay_id"],
                "processed_this_run": state["confirmed_records"] - starting,
                "confirmed_records": state["confirmed_records"], "total_records": manifest["sample_rows"],
                "source_sha256": manifest["sha256"], "last_event_id": state["last_event_id"],
                "delivery": "not_sent_to_kafka" if mode == "dry-run" else "at_least_once"}


def main(argv=None):
    parser = argparse.ArgumentParser(description="Bounded resumable replay of verified G1 historical events; offline by default.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--mode", choices=("dry-run", "kafka"), default="dry-run")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="real_behavior_events_v1")
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--max-events", type=int, default=1000)
    parser.add_argument("--checkpoint-every", type=int, default=100)
    args = parser.parse_args(argv)
    try:
        result = replay_file(args.input, args.checkpoint, mode=args.mode, bootstrap_servers=args.bootstrap_servers,
                             topic=args.topic, rate=args.rate, max_events=args.max_events, checkpoint_every=args.checkpoint_every)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 130 if result["status"] == "paused" else 0
    except KeyboardInterrupt:
        print("replay paused before sending; existing checkpoint preserved", file=sys.stderr)
        return 130
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"replay error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
