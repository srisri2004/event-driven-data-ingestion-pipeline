import json, os
import boto3
from boto3.dynamodb.types import TypeDeserializer

sns = boto3.client("sns")
TOPIC = os.environ["TOPIC_ARN"]
deser = TypeDeserializer()


def handler(event, context):
    failures = []
    for rec in event["Records"]:
        if rec["eventName"] != "INSERT":
            continue
        try:
            item = {k: deser.deserialize(v) for k, v in rec["dynamodb"]["NewImage"].items()}
            sns.publish(
                TopicArn=TOPIC,
                Message=json.dumps(item, default=str),
                MessageAttributes={
                    "event_type": {"DataType": "String", "StringValue": item["event_type"]}
                },
            )
        except Exception as e:
            print("publish error", e)
            failures.append({"itemIdentifier": rec["dynamodb"]["SequenceNumber"]})
    return {"batchItemFailures": failures}
