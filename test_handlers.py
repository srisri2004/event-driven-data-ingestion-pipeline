"""Run: python3 tests/test_handlers.py   (uses stubbed boto3, no AWS needed)"""
import base64, importlib.util, json, os, sys, types, unittest
from decimal import Decimal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------- minimal boto3 / botocore stubs ----------
class ClientError(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}

class FakeKinesis:
    def __init__(self): self.puts = []
    def put_record(self, **kw): self.puts.append(kw)

class FakeSNS:
    def __init__(self): self.msgs = []
    def publish(self, **kw): self.msgs.append(kw)

class FakeTable:
    def __init__(self): self.items = {}; self.fail_with = None
    def put_item(self, Item, ConditionExpression=None):
        if self.fail_with: raise ClientError(self.fail_with)
        if ConditionExpression and Item["event_id"] in self.items:
            raise ClientError("ConditionalCheckFailedException")
        self.items[Item["event_id"]] = Item

class TypeDeserializer:
    def deserialize(self, v):
        (t, x), = v.items()
        if t == "S": return x
        if t == "N": return Decimal(x)
        if t == "BOOL": return x
        if t == "M": return {k: self.deserialize(i) for k, i in x.items()}
        if t == "L": return [self.deserialize(i) for i in x]
        raise ValueError(t)

kinesis, sns, table = FakeKinesis(), FakeSNS(), FakeTable()
boto3 = types.ModuleType("boto3")
boto3.client = lambda svc, **k: {"kinesis": kinesis, "sns": sns}[svc]
boto3.resource = lambda svc, **k: types.SimpleNamespace(Table=lambda name: table)
bd = types.ModuleType("boto3.dynamodb"); bt = types.ModuleType("boto3.dynamodb.types")
bt.TypeDeserializer = TypeDeserializer
bc = types.ModuleType("botocore"); bce = types.ModuleType("botocore.exceptions")
bce.ClientError = ClientError
sys.modules.update({"boto3": boto3, "boto3.dynamodb": bd, "boto3.dynamodb.types": bt,
                    "botocore": bc, "botocore.exceptions": bce})
os.environ.update(STREAM_NAME="s", TABLE_NAME="t", TOPIC_ARN="arn:aws:sns:x:1:t")

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, path))
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

ingest = load("src/ingest/app.py", "ingest")
processor = load("src/processor/app.py", "processor")
publisher = load("src/publisher/app.py", "publisher")
analytics = load("src/workers/analytics.py", "analytics")
alerts = load("src/workers/alerts.py", "alerts")


def kin(rec, seq="1"):
    return {"kinesis": {"sequenceNumber": seq, "data": base64.b64encode(json.dumps(rec).encode()).decode()}}


class Tests(unittest.TestCase):
    def setUp(self):
        kinesis.puts.clear(); sns.msgs.clear(); table.items.clear(); table.fail_with = None

    # --- ingest ---
    def test_ingest_accepts_valid(self):
        r = ingest.handler({"body": json.dumps({"event_type": "error", "payload": {"a": 1}})}, None)
        self.assertEqual(r["statusCode"], 202)
        sent = json.loads(kinesis.puts[0]["Data"])
        self.assertEqual(sent["event_type"], "error")
        self.assertEqual(kinesis.puts[0]["PartitionKey"], sent["event_id"])

    def test_ingest_rejects_bad_type_and_bad_json(self):
        self.assertEqual(ingest.handler({"body": json.dumps({"event_type": "nope"})}, None)["statusCode"], 400)
        self.assertEqual(ingest.handler({"body": "{oops"}, None)["statusCode"], 400)
        self.assertEqual(ingest.handler({"body": None}, None)["statusCode"], 400)
        self.assertEqual(kinesis.puts, [])

    def test_ingest_keeps_client_event_id(self):
        ingest.handler({"body": json.dumps({"event_type": "error", "event_id": "abc"})}, None)
        self.assertEqual(json.loads(kinesis.puts[0]["Data"])["event_id"], "abc")

    # --- processor ---
    def test_processor_writes_and_dedupes(self):
        ev = {"Records": [kin({"event_id": "e1", "event_type": "error", "payload": {"amount": 9.5}}, "1"),
                          kin({"event_id": "e1", "event_type": "error", "payload": {"amount": 9.5}}, "2")]}
        out = processor.handler(ev, None)
        self.assertEqual(out, {"batchItemFailures": []})
        self.assertEqual(len(table.items), 1)
        self.assertIsInstance(table.items["e1"]["payload"]["amount"], Decimal)
        self.assertIn("expires_at", table.items["e1"])

    def test_processor_reports_only_failed_records(self):
        ev = {"Records": [kin({"event_id": "ok", "event_type": "error"}, "1")]}
        ev["Records"].append({"kinesis": {"sequenceNumber": "2", "data": base64.b64encode(b"not json").decode()}})
        out = processor.handler(ev, None)
        self.assertEqual(out["batchItemFailures"], [{"itemIdentifier": "2"}])
        self.assertIn("ok", table.items)

    def test_processor_reports_dynamo_errors(self):
        table.fail_with = "ProvisionedThroughputExceededException"
        out = processor.handler({"Records": [kin({"event_id": "x", "event_type": "error"}, "9")]}, None)
        self.assertEqual(out["batchItemFailures"], [{"itemIdentifier": "9"}])

    # --- publisher ---
    def ddb(self, name="INSERT"):
        return {"eventName": name, "dynamodb": {"SequenceNumber": "77", "NewImage": {
            "event_id": {"S": "e1"}, "event_type": {"S": "payment_failed"},
            "payload": {"M": {"amount": {"N": "12.5"}, "n": {"N": "3"}}}}}}

    def test_publisher_publishes_with_attribute_and_numbers(self):
        out = publisher.handler({"Records": [self.ddb()]}, None)
        self.assertEqual(out["batchItemFailures"], [])
        m = sns.msgs[0]
        self.assertEqual(m["MessageAttributes"]["event_type"]["StringValue"], "payment_failed")
        body = json.loads(m["Message"])
        self.assertEqual(body["payload"], {"amount": 12.5, "n": 3})

    def test_publisher_ignores_remove_and_modify(self):
        publisher.handler({"Records": [self.ddb("REMOVE"), self.ddb("MODIFY")]}, None)
        self.assertEqual(sns.msgs, [])

    # --- workers ---
    def test_workers_batch_failures(self):
        good = {"messageId": "m1", "body": json.dumps({"event_type": "error", "event_id": "e"})}
        bad = {"messageId": "m2", "body": json.dumps({"event_type": "error", "event_id": "e", "payload": {"force_fail": True}})}
        junk = {"messageId": "m3", "body": "not json"}
        out = analytics.handler({"Records": [good, bad, junk]}, None)
        self.assertEqual(out["batchItemFailures"], [{"itemIdentifier": "m2"}, {"itemIdentifier": "m3"}])
        self.assertEqual(alerts.handler({"Records": [good]}, None), {"batchItemFailures": []})
        self.assertEqual(alerts.handler({"Records": [junk]}, None)["batchItemFailures"], [{"itemIdentifier": "m3"}])

    # --- end to end through all stages ---
    def test_pipeline_end_to_end(self):
        ingest.handler({"body": json.dumps({"event_type": "payment_failed", "payload": {"amount": 20.25}})}, None)
        rec = {"kinesis": {"sequenceNumber": "1", "data": base64.b64encode(kinesis.puts[0]["Data"]).decode()}}
        processor.handler({"Records": [rec]}, None)
        item = next(iter(table.items.values()))
        image = {"event_id": {"S": item["event_id"]}, "event_type": {"S": item["event_type"]},
                 "payload": {"M": {"amount": {"N": str(item["payload"]["amount"])}}}}
        publisher.handler({"Records": [{"eventName": "INSERT", "dynamodb": {"SequenceNumber": "1", "NewImage": image}}]}, None)
        msg = {"messageId": "q1", "body": sns.msgs[0]["Message"]}
        self.assertEqual(alerts.handler({"Records": [msg]}, None)["batchItemFailures"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
