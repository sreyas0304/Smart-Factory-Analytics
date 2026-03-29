# simulator/device_simulator.py

import asyncio
import json
import logging
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

STREAM_NAME        = os.getenv("KINESIS_STREAM_NAME", "smart-factory-telemetry")
AWS_REGION         = os.getenv("AWS_REGION", "us-east-1")
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")          # DEBUG shows every record
LOG_TO_FILE        = os.getenv("LOG_TO_FILE", "true").lower() == "true"
LOG_FILE_PATH      = os.getenv("LOG_FILE_PATH", "logs/simulator.log")

ZONES  = ["zone-A", "zone-B", "zone-C"]
ZONE_PARTITION_KEY = {
    "zone-A": "shard-zone-A",
    "zone-B": "shard-zone-B",
    "zone-C": "shard-zone-C",
}

PUBLISH_INTERVAL_S = 0.5
BATCH_FLUSH_SIZE   = 100
BATCH_FLUSH_SECS   = 1.0


# ── Logger setup ──────────────────────────────────────────────────────────────

def setup_logger() -> logging.Logger:
    """
    Sets up a structured logger that writes to both console and a rotating
    log file. Every log line is valid JSON so it can be ingested by
    CloudWatch, Datadog, or any log aggregator directly.
    """
    os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)

    logger = logging.getLogger("simulator")
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    # Prevent duplicate handlers if this is called more than once
    if logger.handlers:
        return logger

    class JsonFormatter(logging.Formatter):
        """Formats every log record as a single-line JSON object."""
        def format(self, record: logging.LogRecord) -> str:
            log_obj = {
                "ts":      datetime.now(timezone.utc).isoformat(),
                "level":   record.levelname,
                "logger":  record.name,
                "message": record.getMessage(),
            }
            # If the caller passed a dict as the message, merge it in
            if isinstance(record.msg, dict):
                log_obj.update(record.msg)
                log_obj["message"] = record.levelname.lower()

            # Attach any extra= fields passed to the log call
            for key, val in record.__dict__.items():
                if key not in (
                    "msg", "args", "levelname", "levelno", "pathname",
                    "filename", "module", "exc_info", "exc_text",
                    "stack_info", "lineno", "funcName", "created",
                    "msecs", "relativeCreated", "thread", "threadName",
                    "processName", "process", "name", "message",
                ):
                    log_obj[key] = val

            return json.dumps(log_obj, default=str)

    formatter = JsonFormatter()

    # Console handler — always on
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler — controlled by LOG_TO_FILE env var
    if LOG_TO_FILE:
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            LOG_FILE_PATH,
            maxBytes    = 10 * 1024 * 1024,   # rotate at 10 MB
            backupCount = 5,                   # keep 5 rotated files
            encoding    = "utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"File logging enabled → {LOG_FILE_PATH}")

    return logger


log = setup_logger()


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class SensorReading:
    event_id:           str
    device_id:          str
    device_type:        str
    zone:               str
    shift:              str
    event_time:         str
    publish_time:       str
    sequence_number:    int
    vibration_x:        float = 0.0
    vibration_y:        float = 0.0
    spindle_rpm:        float = 0.0
    tool_wear_pct:      float = 0.0
    belt_speed_mps:     float = 0.0
    motor_current_amps: float = 0.0
    temperature_c:      float = 0.0
    humidity_pct:       float = 0.0
    is_malformed:       bool  = False
    was_buffered:       bool  = False


# ── Kinesis batch publisher ───────────────────────────────────────────────────

class KinesisPublisher:

    MAX_BATCH = 500

    def __init__(self):
        self.client = boto3.client(
            "kinesis",
            region_name           = os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self._batch:        list[dict] = []
        self._lock                     = asyncio.Lock()
        self._total_sent               = 0
        self._total_failed             = 0
        self._total_retried            = 0

    def _make_entry(self, record: SensorReading) -> dict:
        payload = asdict(record)
        return {
            "Data":         json.dumps(payload).encode("utf-8"),
            "PartitionKey": ZONE_PARTITION_KEY[record.zone],
            "_record":      payload,     # kept for logging, stripped before API call
        }

    async def add(self, record: SensorReading):
        async with self._lock:
            self._batch.append(self._make_entry(record))
            if len(self._batch) >= self.MAX_BATCH:
                await self._flush_locked()

    async def flush(self):
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self):
        if not self._batch:
            return

        batch = self._batch[:]
        self._batch.clear()

        # ── Log every record at DEBUG level before sending ────────────────────
        for entry in batch:
            record_payload = entry.pop("_record", {})   # remove before API call
            log.debug({
                "event":        "record_queued",
                "stream":       STREAM_NAME,
                "device_id":    record_payload.get("device_id"),
                "device_type":  record_payload.get("device_type"),
                "zone":         record_payload.get("zone"),
                "shift":        record_payload.get("shift"),
                "event_time":   record_payload.get("event_time"),
                "sequence_number": record_payload.get("sequence_number"),
                "is_anomaly":   (
                    record_payload.get("vibration_x", 0) > 8.0 or
                    record_payload.get("vibration_y", 0) > 8.0 or
                    record_payload.get("motor_current_amps", 0) > 20.0 or
                    record_payload.get("temperature_c", 0) > 55.0
                ),
                "was_buffered": record_payload.get("was_buffered"),
                "is_malformed": record_payload.get("is_malformed"),
                "payload":      record_payload,    # full JSON record
            })

        # ── Send to Kinesis ───────────────────────────────────────────────────
        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.put_records(
                    StreamName = STREAM_NAME,
                    Records    = batch,
                ),
            )
        except ClientError as e:
            log.error({
                "event":        "kinesis_api_error",
                "stream":       STREAM_NAME,
                "batch_size":   len(batch),
                "error":        str(e),
            })
            self._total_failed += len(batch)
            return

        failed = resp.get("FailedRecordCount", 0)
        sent   = len(batch) - failed
        self._total_sent += sent

        # ── Log the batch result at INFO level ────────────────────────────────
        log.info({
            "event":      "batch_flushed",
            "stream":     STREAM_NAME,
            "sent":       sent,
            "failed":     failed,
            "batch_size": len(batch),
        })

        # ── Retry failed records once ─────────────────────────────────────────
        if failed > 0:
            retry_entries = [
                batch[i] for i, r in enumerate(resp["Records"])
                if "ErrorCode" in r
            ]
            failed_reasons = list({
                r.get("ErrorCode", "unknown")
                for r in resp["Records"]
                if "ErrorCode" in r
            })
            log.warning({
                "event":         "batch_partial_failure",
                "stream":        STREAM_NAME,
                "failed_count":  failed,
                "error_codes":   failed_reasons,
                "retrying":      len(retry_entries),
            })

            await asyncio.sleep(0.5)
            try:
                self.client.put_records(StreamName=STREAM_NAME, Records=retry_entries)
                self._total_retried  += len(retry_entries)
                self._total_failed   -= len(retry_entries)   # recovered
                log.info({
                    "event":    "retry_succeeded",
                    "retried":  len(retry_entries),
                })
            except ClientError as e:
                log.error({
                    "event":  "retry_failed",
                    "error":  str(e),
                    "lost":   len(retry_entries),
                })

    def stats(self) -> dict:
        return {
            "sent":    self._total_sent,
            "failed":  self._total_failed,
            "retried": self._total_retried,
        }


# ── Device simulator ──────────────────────────────────────────────────────────

class DeviceSimulator:
    def __init__(self, device_id: str, device_type: str, zone: str):
        self.device_id          = device_id
        self.device_type        = device_type
        self.zone               = zone
        self.seq                = 0
        self._offline_until:    float = 0.0
        self._buffered_events:  list  = []

    @staticmethod
    def current_shift() -> str:
        h = datetime.now().hour
        if 6 <= h < 14:  return "morning"
        if 14 <= h < 22: return "afternoon"
        return "night"

    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    def _cnc_reading(self, event_time: str, anomaly: bool) -> SensorReading:
        r = SensorReading(
            event_id        = str(uuid.uuid4()),
            device_id       = self.device_id,
            device_type     = self.device_type,
            zone            = self.zone,
            shift           = self.current_shift(),
            event_time      = event_time,
            publish_time    = datetime.now(timezone.utc).isoformat(),
            sequence_number = self._next_seq(),
            vibration_x     = round(random.gauss(2.5, 0.3), 3),
            vibration_y     = round(random.gauss(2.5, 0.3), 3),
            spindle_rpm     = round(random.gauss(3000, 50), 1),
            tool_wear_pct   = round(min(100.0, self.seq * 0.02 + random.uniform(5, 20)), 2),
        )
        if anomaly:
            r.vibration_x = round(random.uniform(8.0, 15.0), 3)
            r.vibration_y = round(random.uniform(8.0, 15.0), 3)
            log.warning({
                "event":       "anomaly_injected",
                "device_id":   r.device_id,
                "device_type": r.device_type,
                "zone":        r.zone,
                "vibration_x": r.vibration_x,
                "vibration_y": r.vibration_y,
                "seq":         r.sequence_number,
            })
        return r

    def _conveyor_reading(self, event_time: str, anomaly: bool) -> SensorReading:
        r = SensorReading(
            event_id           = str(uuid.uuid4()),
            device_id          = self.device_id,
            device_type        = self.device_type,
            zone               = self.zone,
            shift              = self.current_shift(),
            event_time         = event_time,
            publish_time       = datetime.now(timezone.utc).isoformat(),
            sequence_number    = self._next_seq(),
            belt_speed_mps     = round(random.gauss(1.2, 0.05), 3),
            motor_current_amps = round(random.gauss(12.0, 0.5), 2),
        )
        if anomaly:
            r.motor_current_amps = round(random.uniform(25.0, 40.0), 2)
            log.warning({
                "event":               "anomaly_injected",
                "device_id":           r.device_id,
                "device_type":         r.device_type,
                "zone":                r.zone,
                "motor_current_amps":  r.motor_current_amps,
                "seq":                 r.sequence_number,
            })
        return r

    def _temperature_reading(self, event_time: str, anomaly: bool) -> SensorReading:
        r = SensorReading(
            event_id        = str(uuid.uuid4()),
            device_id       = self.device_id,
            device_type     = self.device_type,
            zone            = self.zone,
            shift           = self.current_shift(),
            event_time      = event_time,
            publish_time    = datetime.now(timezone.utc).isoformat(),
            sequence_number = self._next_seq(),
            temperature_c   = round(random.gauss(22.0, 1.0), 2),
            humidity_pct    = round(random.gauss(55.0, 3.0), 2),
        )
        if anomaly:
            r.temperature_c = round(random.uniform(60.0, 90.0), 2)
            log.warning({
                "event":         "anomaly_injected",
                "device_id":     r.device_id,
                "device_type":   r.device_type,
                "zone":          r.zone,
                "temperature_c": r.temperature_c,
                "seq":           r.sequence_number,
            })
        return r

    def generate(self, event_time: str = None, anomaly: bool = False) -> SensorReading:
        t = event_time or datetime.now(timezone.utc).isoformat()
        if self.device_type == "cnc":
            return self._cnc_reading(t, anomaly)
        if self.device_type == "conveyor":
            return self._conveyor_reading(t, anomaly)
        return self._temperature_reading(t, anomaly)

    def malformed(self) -> dict:
        self.seq += 1
        payload = {
            "event_id":        str(uuid.uuid4()),
            "device_id":       self.device_id,
            "device_type":     self.device_type,
            "zone":            self.zone,
            "event_time":      "NOT_A_TIMESTAMP",
            "sequence_number": self.seq,
            "is_malformed":    True,
        }
        log.warning({
            "event":       "malformed_payload_sent",
            "device_id":   self.device_id,
            "device_type": self.device_type,
            "zone":        self.zone,
            "seq":         self.seq,
            "payload":     payload,
        })
        return payload

    async def run(self, publisher: KinesisPublisher):
        while True:
            now = time.time()

            # Device comes back online — flush buffered OOO events
            if self._offline_until > 0 and now > self._offline_until:
                log.info({
                    "event":          "device_reconnected",
                    "device_id":      self.device_id,
                    "buffered_count": len(self._buffered_events),
                })
                for buffered in self._buffered_events:
                    buffered.was_buffered = True
                    buffered.publish_time = datetime.now(timezone.utc).isoformat()
                    await publisher.add(buffered)
                    await asyncio.sleep(0.02)
                self._buffered_events.clear()
                self._offline_until = 0.0

            # Device is offline — buffer locally
            if self._offline_until > now:
                event_time = datetime.now(timezone.utc).isoformat()
                reading    = self.generate(event_time, self._maybe_anomaly())
                self._buffered_events.append(reading)
                await asyncio.sleep(PUBLISH_INTERVAL_S)
                continue

            # Randomly go offline
            if random.random() < 0.001:
                offline_secs            = random.uniform(10, 90)
                self._offline_until     = now + offline_secs
                log.info({
                    "event":        "device_offline",
                    "device_id":    self.device_id,
                    "offline_secs": round(offline_secs, 1),
                })
                await asyncio.sleep(PUBLISH_INTERVAL_S)
                continue

            # Randomly emit a malformed payload
            if random.random() < 0.0005:
                payload = json.dumps(self.malformed()).encode("utf-8")
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda p=payload: publisher.client.put_record(
                        StreamName   = STREAM_NAME,
                        Data         = p,
                        PartitionKey = ZONE_PARTITION_KEY[self.zone],
                    ),
                )
                await asyncio.sleep(PUBLISH_INTERVAL_S)
                continue

            # Normal publish
            reading = self.generate(anomaly=self._maybe_anomaly())
            await publisher.add(reading)
            await asyncio.sleep(PUBLISH_INTERVAL_S)

    @staticmethod
    def _maybe_anomaly() -> bool:
        return random.random() < 0.02


# ── Fleet ─────────────────────────────────────────────────────────────────────

def build_fleet() -> list[DeviceSimulator]:
    devices = []
    for i in range(50):
        devices.append(DeviceSimulator(f"cnc-{i:03d}",  "cnc",         ZONES[i % 3]))
    for i in range(20):
        devices.append(DeviceSimulator(f"conv-{i:03d}", "conveyor",    ZONES[i % 3]))
    for i in range(30):
        devices.append(DeviceSimulator(f"temp-{i:03d}", "temperature", ZONES[i % 3]))
    return devices


# ── Periodic stats logger ─────────────────────────────────────────────────────

async def periodic_flush(publisher: KinesisPublisher):
    tick = 0
    while True:
        await asyncio.sleep(1.0)
        await publisher.flush()
        tick += 1
        if tick % 10 == 0:
            s = publisher.stats()
            log.info({
                "event":       "throughput_stats",
                "sent_total":  s["sent"],
                "failed_total":s["failed"],
                "retried_total":s["retried"],
                "approx_rps":  s["sent"] // tick,
            })


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    log.info({
        "event":    "simulator_starting",
        "stream":   STREAM_NAME,
        "region":   AWS_REGION,
        "devices":  100,
        "cadence":  f"{PUBLISH_INTERVAL_S}s",
        "log_level":LOG_LEVEL,
        "log_file": LOG_FILE_PATH if LOG_TO_FILE else "disabled",
    })

    publisher = KinesisPublisher()
    fleet     = build_fleet()

    tasks = [asyncio.create_task(d.run(publisher)) for d in fleet]
    tasks.append(asyncio.create_task(periodic_flush(publisher)))

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info({"event": "simulator_shutdown", "final_stats": publisher.stats()})
        await publisher.flush()


if __name__ == "__main__":
    asyncio.run(main())