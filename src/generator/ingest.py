# import json
# import time
# import random
# import uuid
# from datetime import datetime, timezone
# import boto3

# # Configuration
# STREAM_NAME = "smart-factory-telemetry"
# AWS_REGION = "us-east-1"
# PUBLISH_INTERVAL_S = 1.0

# def generate_reading(device_id, device_type, zone):
#     """Generates a simple, randomized factory sensor reading."""
#     return {
#         "event_id": str(uuid.uuid4()),
#         "device_id": device_id,
#         "device_type": device_type,
#         "zone": zone,
#         "event_time": datetime.now(timezone.utc).isoformat(),
#         "temperature_c": round(random.gauss(22.0, 1.0), 2),
#         "vibration_x": round(random.gauss(2.5, 0.3), 3),
#         "motor_current_amps": round(random.gauss(12.0, 0.5), 2),
#     }

# def main():
#     print(f"Starting simple Kinesis publisher for stream: {STREAM_NAME}\n" + "="*50)
    
#     # Initialize the Kinesis client
#     kinesis = boto3.client('kinesis', region_name=AWS_REGION)

#     # Define a small fleet of devices spread across 3 zones
#     # The 3 distinct zones will serve as our partition keys for the 3 shards
#     devices = [
#         ("cnc-001", "cnc", "zone-A"),
#         ("conv-001", "conveyor", "zone-B"),
#         ("temp-001", "temperature", "zone-C"),
#     ]

#     try:
#         while True:
#             records = []
            
#             # 1. Generate a reading for each device
#             for device_id, device_type, zone in devices:
#                 reading = generate_reading(device_id, device_type, zone)

#                 # --- NEW: Print each individual record to the terminal ---
#                 print(f"Record ({device_id}): {json.dumps(reading)}")

#                 # 2. Format the payload for the Kinesis put_records API
#                 records.append({
#                     "Data": (json.dumps(reading) + "\n").encode("utf-8"),
#                     # "PartitionKey": f"shard-{zone}"
#                     "PartitionKey": reading["event_id"]
#                 })

#             # 3. Send the batch to Kinesis
#             response = kinesis.put_records(
#                 StreamName=STREAM_NAME,
#                 Records=records
#             )

#             # 4. Log the batch results
#             failed = response.get('FailedRecordCount', 0)
#             sent = len(records) - failed
#             print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch Status -> Sent: {sent} | Failed: {failed}")
#             print("-" * 50) # Visual separator for the next batch

#             # 5. Wait before sending the next batch
#             time.sleep(PUBLISH_INTERVAL_S)

#     except KeyboardInterrupt:
#         print("\nPublisher stopped by user.")
#     except Exception as e:
#         print(f"\nAn error occurred: {e}")

# if __name__ == "__main__":
#     main()

import json
import time
import random
import uuid
from datetime import datetime, timezone
import boto3

STREAM_NAME = "smart-factory-telemetry"
AWS_REGION = "us-east-1"
PUBLISH_INTERVAL_S = 1.0

def generate_reading(device_id, device_type, zone):
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "device_type": device_type,
        "zone": zone,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(random.gauss(22.0, 1.0), 2),
        "vibration_x": round(random.gauss(2.5, 0.3), 3),
        "motor_current_amps": round(random.gauss(12.0, 0.5), 2),
    }

def main():
    print(f"Starting Kinesis publisher for stream: {STREAM_NAME}\n" + "="*50)
    kinesis = boto3.client('kinesis', region_name=AWS_REGION)

    devices = [
        ("cnc-001", "cnc", "zone-A"),
        ("conv-001", "conveyor", "zone-B"),
        ("temp-001", "temperature", "zone-C"),
    ]

    loop_counter = 0

    try:
        while True:
            records = []
            loop_counter += 1
            
            # Generate normal readings
            for device_id, device_type, zone in devices:
                reading = generate_reading(device_id, device_type, zone)
                records.append({
                    "Data": (json.dumps(reading) + "\n").encode("utf-8"),
                    "PartitionKey": reading["event_id"]
                })

            # --- MODIFIED: Aggressive Bad Record Injection ---
            # 30% random chance OR strictly enforced every 5th loop
            if random.random() < 0.30 or loop_counter % 5 == 0:
                # Type 1: Missing 'device_type'
                bad_json = {"event_id": str(uuid.uuid4()), "status": "corrupt", "value": 99.9}
                records.append({
                    "Data": (json.dumps(bad_json) + "\n").encode("utf-8"),
                    "PartitionKey": bad_json["event_id"]
                })
                print("⚠️  Injected record missing device_type")

            # 30% random chance OR strictly enforced every 7th loop
            if random.random() < 0.30 or loop_counter % 7 == 0:
                # Type 2: Completely malformed string (Not JSON)
                bad_string = f"CRITICAL_ERROR_SENSOR_OFFLINE_ID_{uuid.uuid4()}"
                records.append({
                    "Data": (bad_string + "\n").encode("utf-8"),
                    "PartitionKey": str(uuid.uuid4())
                })
                print("⚠️  Injected malformed string record")

            # Send the batch
            response = kinesis.put_records(StreamName=STREAM_NAME, Records=records)
            failed = response.get('FailedRecordCount', 0)
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch Status -> Sent: {len(records) - failed} | Failed: {failed}")
            print("-" * 50)
            time.sleep(PUBLISH_INTERVAL_S)

    except KeyboardInterrupt:
        print("\nPublisher stopped by user.")

if __name__ == "__main__":
    main()

