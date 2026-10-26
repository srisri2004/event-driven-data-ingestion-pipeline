# Event-Driven Data Ingestion (AWS)

```
Client -> API Gateway -> Ingest Lambda -> Kinesis Data Stream
       -> Processor Lambda (validate, idempotent write) -> DynamoDB
       -> DynamoDB Streams -> Publisher Lambda -> SNS topic
            |-> SQS analytics queue (all events)  -> worker (DLQ after 3 tries)
            |-> SQS alerts queue (error/payment_failed filter) -> worker (DLQ)
CloudWatch alarm on DLQ depth
```

## Prerequisites
AWS account, AWS CLI (`aws configure`), AWS SAM CLI, Python 3.12.

## Deploy
```
sam build
sam deploy --guided      # stack name: event-ingestion, accept defaults
```

## Test
```
python scripts/send_events.py <ApiUrl from outputs> 20
sam logs -n AlertsWorker --stack-name event-ingestion --tail
aws dynamodb scan --table-name <TableName> --max-items 5
```
DLQ test: POST an event with `"payload": {"force_fail": true}` and watch it land in AnalyticsDLQ after 3 attempts.

## Clean up (avoid charges)
```
sam delete
```

## Design notes (for interviews)
- At-least-once delivery everywhere -> idempotent writes via conditional put on event_id.
- Partial batch responses so one bad record doesn't retry the whole batch.
- SNS->SQS fan-out with filter policies decouples consumers; DLQs isolate poison messages.
- On-demand DynamoDB + TTL keeps cost and storage bounded.
- Next steps: Firehose -> S3 archive + Athena, X-Ray tracing, API auth (Cognito/API keys), CI/CD with GitHub Actions, load test with Artillery.
