import json


def handler(event, context):
    failures = []
    for rec in event["Records"]:
        try:
            msg = json.loads(rec["body"])
            if msg.get("payload", {}).get("force_fail"):  # for DLQ testing
                raise ValueError("forced failure")
            print("ANALYTICS", msg["event_type"], msg["event_id"])
            # TODO: aggregate into a metrics table / S3 / Redshift
        except Exception as e:
            print("analytics error", e)
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
