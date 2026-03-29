import base64
import json
import os
import boto3
from datetime import datetime, timezone

# Initialize clients outside the handler for reuse
firehose_client = boto3.client('firehose')
sqs_client = boto3.client('sqs')

# Mapping device types to their respective Firehose Delivery Stream names
FIREHOSE_STREAMS = {
    'cnc': os.environ.get('FIREHOSE_CNC', 'fiehose-cnc'),
    'conveyor': os.environ.get('FIREHOSE_CONVEYOR', 'firehose-conveyor'),
    'temperature': os.environ.get('FIREHOSE_TEMPERATURE', 'firehose-temperature')
}

# Your Dead Letter Queue URL
DLQ_URL = os.environ.get('DLQ_SQS_URL', 'https://sqs.us-east-1.amazonaws.com/YOUR_ACCOUNT_ID/smart-factory-dlq')

def lambda_handler(event, context):
    # Dictionaries to hold batches for each destination
    firehose_batches = { 'cnc': [], 'conveyor': [], 'temperature': [] }
    dlq_batch = []
    
    for record in event['Records']:
        try:
            # 1. Decode Kinesis Data
            payload_base64 = record['kinesis']['data']
            payload_str = base64.b64decode(payload_base64).decode('utf-8')
            
            # 2. Attempt to parse JSON
            payload_json = json.loads(payload_str)
            
            # 3. Validate device_type for routing
            device_type = payload_json.get('device_type')
            if not device_type or device_type not in FIREHOSE_STREAMS:
                # Valid JSON, but missing/unknown routing key -> Send to DLQ
                dlq_batch.append({
                    'Id': str(len(dlq_batch)), 
                    'MessageBody': payload_str
                })
                continue
                
            # 4. Ensure timestamp exists (add processed time if event_time is missing)
            if 'event_time' not in payload_json:
                payload_json['event_time'] = datetime.now(timezone.utc).isoformat()
                
            # 5. Format and append to the correct Firehose batch
            final_payload = json.dumps(payload_json) + "\n"
            firehose_batches[device_type].append({
                'Data': final_payload.encode('utf-8')
            })
            
        except json.JSONDecodeError:
            # Completely malformed data (Not JSON) -> Send to DLQ
            raw_data = base64.b64decode(record['kinesis']['data']).decode('utf-8', errors='ignore')
            dlq_batch.append({
                'Id': str(len(dlq_batch)), 
                'MessageBody': f"INVALID_JSON: {raw_data}"
            })
        except Exception as e:
            print(f"Unexpected error: {str(e)}")

    # --- EXECUTE BATCH SENDS ---
    
    # Send valid data to respective Firehose streams
    for dtype, batch in firehose_batches.items():
        if batch:
            firehose_client.put_record_batch(
                DeliveryStreamName=FIREHOSE_STREAMS[dtype], 
                Records=batch
            )
            print(f"Sent {len(batch)} records to {FIREHOSE_STREAMS[dtype]}")
            
    # Send invalid data to SQS Dead Letter Queue
    if dlq_batch:
        # SQS put_message_batch has a max of 10 items per API call
        for i in range(0, len(dlq_batch), 10):
            sqs_client.send_message_batch(
                QueueUrl=DLQ_URL, 
                Entries=dlq_batch[i:i+10]
            )
        print(f"Sent {len(dlq_batch)} bad records to DLQ")
        
    return {"status": "success"}
