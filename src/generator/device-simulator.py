# simulator/device_simulator.py

import asyncio
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

STREAM_NAME   = os.getenv("KINESIS_STREAM_NAME", "smart-factory-telemetry")
AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")

ZONES  = ["zone-A", "zone-B", "zone-C"]

# Partition key maps zone → shard affinity
ZONE_PARTITION_KEY = {
    "zone-A": "shard-zone-A",
    "zone-B": "shard-zone-B",
    "zone-C": "shard-zone-C",
}

PUBLISH_INTERVAL_S  = 0.5    # 500ms per device
BATCH_FLUSH_SIZE    = 100    # flush Kinesis batch after this many records
BATCH_FLUSH_SECS    = 1.0    # or after this many seconds, whichever comes first


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class SensorReading:
    event_id:          str
    device_id:         str
    device_type:       str        # cnc | conveyor | temperature
    zone:              str
    shift:             str
    event_time:        str        # ISO8601 — device clock (may be in the past for OOO)
    publish_time:      str        # wall clock at publish moment
    sequence_number:   int
    # CNC fields
    vibration_x:       float = 0.0
    vibration_y:       float = 0.0
    spindle_rpm:       float = 0.0
    tool_wear_pct:     float = 0.0
    # Conveyor fields
    belt_speed_mps:    float = 0.0
    motor_current_amps:float = 0.0
    # Temperature fields
    temperature_c:     float = 0.0
    humidity_pct:      float = 0.0
    # Pipeline metadata
    is_malformed:      bool  = False
    was_buffered:      bool  = False    # True for OOO events


# ── Kinesis batch publisher ───────────────────────────────────────────────────

class KinesisPublisher:
    """
    Collects records and flushes in batches of up to 500 using PutRecords.
    PutRecords is ~10× more efficient than individual PutRecord calls.
    Handles partial failures — Kinesis PutRecords can fail individual records
    within a successful batch.
    """

    MAX_BATCH = 500

    def __init__(self):
        self.client = boto3.client(
            "kinesis",
            region_name          = os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id    = os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key= os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self._batch: list[dict] = []
        self._lock = asyncio.Lock()
        self._total_sent  = 0
        self._total_failed= 0

    def _make_entry(self, record: SensorReading) -> dict:
        return {
            "Data": json.dumps(asdict(record)).encode("utf-8"),
            "PartitionKey": ZONE_PARTITION_KEY[record.zone],
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

        try:
            resp = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.client.put_records(
                    StreamName=STREAM_NAME,
                    Records=batch,
                ),
            )
        except ClientError as e:
            print(f"[Kinesis] PutRecords API error: {e}")
            self._total_failed += len(batch)
            return

        failed = resp.get("FailedRecordCount", 0)
        self._total_sent   += len(batch) - failed
        self._total_failed += failed

        # Retry individual failed records once with exponential backoff
        if failed > 0:
            retry = [
                batch[i] for i, r in enumerate(resp["Records"])
                if "ErrorCode" in r
            ]
            await asyncio.sleep(0.5)
            try:
                self.client.put_records(StreamName=STREAM_NAME, Records=retry)
                self._total_failed -= len(retry)   # recovered
            except ClientError:
                pass   # give up — DLQ will catch downstream

    def stats(self) -> dict:
        return {"sent": self._total_sent, "failed": self._total_failed}


# ── Device simulator ──────────────────────────────────────────────────────────

class DeviceSimulator:
    def __init__(self, device_id: str, device_type: str, zone: str):
        self.device_id   = device_id
        self.device_type = device_type
        self.zone        = zone
        self.seq         = 0
        # OOO state
        self._offline_until:    float = 0.0
        self._buffered_events:  list  = []

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def current_shift() -> str:
        h = datetime.now().hour
        if 6 <= h < 14:   return "morning"
        if 14 <= h < 22:  return "afternoon"
        return "night"

    def _next_seq(self) -> int:
        self.seq += 1
        return self.seq

    # ── Reading generators ────────────────────────────────────────────────────

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
        return r

    def _temperature_reading(self, event_time: str, anomaly: bool) -> SensorReading:
        r = SensorReading(
            event_id      = str(uuid.uuid4()),
            device_id     = self.device_id,
            device_type   = self.device_type,
            zone          = self.zone,
            shift         = self.current_shift(),
            event_time    = event_time,
            publish_time  = datetime.now(timezone.utc).isoformat(),
            sequence_number = self._next_seq(),
            temperature_c = round(random.gauss(22.0, 1.0), 2),
            humidity_pct  = round(random.gauss(55.0, 3.0), 2),
        )
        if anomaly:
            r.temperature_c = round(random.uniform(60.0, 90.0), 2)
        return r

    def generate(self, event_time: str = None, anomaly: bool = False) -> SensorReading:
        t = event_time or datetime.now(timezone.utc).isoformat()
        if self.device_type == "cnc":
            return self._cnc_reading(t, anomaly)
        if self.device_type == "conveyor":
            return self._conveyor_reading(t, anomaly)
        return self._temperature_reading(t, anomaly)

    def malformed(self) -> dict:
        """Returns a raw dict with a deliberately bad field — triggers DLQ."""
        self.seq += 1
        return {
            "event_id":        str(uuid.uuid4()),
            "device_id":       self.device_id,
            "device_type":     self.device_type,
            "zone":            self.zone,
            "event_time":      "NOT_A_TIMESTAMP",      # firmware bug
            "sequence_number": self.seq,
            "is_malformed":    True,
        }

    # ── Async run loop ────────────────────────────────────────────────────────

    async def run(self, publisher: "KinesisPublisher"):
        while True:
            now = time.time()

            # ── Device comes back online: flush buffered OOO events ──────────
            if self._offline_until > 0 and now > self._offline_until:
                print(
                    f"  [OOO] {self.device_id} back online — "
                    f"flushing {len(self._buffered_events)} buffered events"
                )
                for buffered in self._buffered_events:
                    buffered.was_buffered = True
                    buffered.publish_time = datetime.now(timezone.utc).isoformat()
                    await publisher.add(buffered)
                    await asyncio.sleep(0.02)   # rapid burst, not 500ms
                self._buffered_events.clear()
                self._offline_until = 0.0

            # ── Device is currently offline: buffer events locally ───────────
            if self._offline_until > now:
                # Use the real event time (device clock keeps ticking offline)
                event_time = datetime.now(timezone.utc).isoformat()
                reading = self.generate(event_time, self._maybe_anomaly())
                self._buffered_events.append(reading)
                await asyncio.sleep(PUBLISH_INTERVAL_S)
                continue

            # ── Randomly go offline (0.1% chance per tick) ───────────────────
            if random.random() < 0.001:
                offline_secs = random.uniform(10, 90)
                self._offline_until = now + offline_secs
                print(f"  [OOO] {self.device_id} going offline for {offline_secs:.0f}s")
                await asyncio.sleep(PUBLISH_INTERVAL_S)
                continue

            # ── Randomly emit a malformed payload (0.05% chance) ────────────
            if random.random() < 0.0005:
                payload = json.dumps(self.malformed()).encode("utf-8")
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda p=payload: publisher.client.put_record(
                        StreamName=STREAM_NAME,
                        Data=p,
                        PartitionKey=ZONE_PARTITION_KEY[self.zone],
                    ),
                )
                print(f"  [DLQ-bait] {self.device_id} sent malformed payload")
                await asyncio.sleep(PUBLISH_INTERVAL_S)
                continue

            # ── Normal publish ────────────────────────────────────────────────
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


# ── Periodic batch flusher ────────────────────────────────────────────────────

async def periodic_flush(publisher: KinesisPublisher):
    """
    Flushes any un-batched records every second so small batches
    aren't stuck waiting to reach BATCH_FLUSH_SIZE.
    Also prints a throughput summary every 10 seconds.
    """
    tick = 0
    while True:
        await asyncio.sleep(1.0)
        await publisher.flush()
        tick += 1
        if tick % 10 == 0:
            s = publisher.stats()
            print(
                f"[Stats] sent={s['sent']:,}  failed={s['failed']:,}  "
                f"throughput≈{s['sent'] // tick}/s"
            )


# ── Entry point ───────────────────────────────────────────────────────────────

async def main():
    print(f"Starting smart-factory simulator")
    print(f"  Stream : {STREAM_NAME}")
    print(f"  Region : {AWS_REGION}")
    print(f"  Devices: 100 (50 CNC + 20 conveyors + 30 temp sensors)")
    print(f"  Cadence: {PUBLISH_INTERVAL_S}s per device\n")

    publisher = KinesisPublisher()
    fleet     = build_fleet()

    tasks = [asyncio.create_task(d.run(publisher)) for d in fleet]
    tasks.append(asyncio.create_task(periodic_flush(publisher)))

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("\nShutting down — flushing remaining records...")
        await publisher.flush()
        print(f"Final stats: {publisher.stats()}")


if __name__ == "__main__":
    asyncio.run(main())