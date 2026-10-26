import json


def handler(event, context):
    failures = []
    for rec in event["Records"]:
        try:
            msg = json.loads(rec["body"])
            print("ALERT!", msg["event_type"], msg["event_id"])
            # TODO: send email via SES / Slack webhook / PagerDuty
        except Exception as e:
            print("alert error", e)
            failures.append({"itemIdentifier": rec["messageId"]})
    return {"batchItemFailures": failures}
