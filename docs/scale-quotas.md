# Relay scale quotas (us-west-2)

> Researched 2026-11-09 from official documentation. Rows marked "unverified" still need confirming, and every value must be re-checked in the Service Quotas console of the real account before a request is filed.

Target load: 1M pushes in 60-120 s (10M statewide), then about 1M check-ins in 5 min with 20-50k req/s peaks.

Derived rates: 1M / 60 s = 16,667 sends/s; 1M / 120 s = 8,333 sends/s; 10M / 120 s = 83,333 sends/s.

"Unverified" means the figure was not found on an official page in this pass.

## Lambda

| Limit                              | Default                                     | Adjustable | Request             | Why                                                                     |
| ---------------------------------- | ------------------------------------------- | ---------- | ------------------- | ----------------------------------------------------------------------- |
| Concurrent executions (per Region) | 1,000                                       | Yes        | 15,000              | 50k req/s x 0.2 s average = 10k concurrent, plus headroom               |
| Function concurrency scaling       | 1,000 environments every 10 s, per function | No         | n/a (pre-provision) | 1k to 10k takes about 90 s; use provisioned concurrency before the send |
| Sync payload                       | 6 MB                                        | No         | n/a                 | Not a constraint for check-ins                                          |

Source: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html

## Lambda SQS event source mapping (standard queues)

| Limit                   | Default                                | Adjustable                           | Request | Why                                                             |
| ----------------------- | -------------------------------------- | ------------------------------------ | ------- | --------------------------------------------------------------- |
| Initial concurrency     | 5 invocations                          | n/a                                  | n/a     | Starting point                                                  |
| Scale-up rate           | 300 more concurrent invokes per minute | No (documented)                      | n/a     | 5 to 1,000 takes about 3.3 min, longer than the 60-120 s window |
| Max concurrency ceiling | 1,250                                  | Yes, `MaximumConcurrency` 2 to 1,000 | 1,000   | Set explicitly so the cap is intentional                        |

Sources: https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-scaling.html

## API Gateway HTTP API

| Limit                                   | Default                 | Adjustable                   | Request    | Why                                            |
| --------------------------------------- | ----------------------- | ---------------------------- | ---------- | ---------------------------------------------- |
| Throttle rate (per account, per Region) | 10,000 RPS              | Yes (code L-8A5B8E43)        | 60,000 RPS | 50k peak plus headroom                         |
| Burst (token bucket)                    | 5,000                   | No (set by the service team) | n/a        | Spikes above 10k RPS drain this bucket quickly |
| Integration timeout, payload size       | Not on the page checked | Unverified                   | n/a        | Check the HTTP API quotas page                 |

Source: https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html

## DynamoDB

| Limit                                 | Default                                                                                                    | Adjustable            | Request                               | Why                                                    |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------- | --------------------- | ------------------------------------- | ------------------------------------------------------ |
| On-demand throughput per table        | 40,000 RRU and 40,000 WRU                                                                                  | Yes                   | 120,000 WRU and 120,000 RRU           | 50k peak x 2 writes (claim plus record) x 1.2 headroom |
| Initial capacity, new on-demand table | 4,000 WRU and 12,000 RRU                                                                                   | No                    | Pre-warm                              | Reaching 100k needs several doubling steps             |
| Previous-peak rule                    | Up to 2x previous peak instantly; throttling if growth exceeds 2x within 30 min of reaching the prior peak | No                    | Pre-warm, or space growth over 30 min | Load test before the send                              |
| Provisioned per table / per account   | 40,000 / 80,000                                                                                            | Yes                   | Only if provisioned mode is used      | Alternative to on-demand                               |
| Tables per account per Region         | 2,500                                                                                                      | Yes, via account team | n/a                                   | Only if sharding into many tables                      |
| Per-partition limits                  | Unverified (commonly cited 3,000 RCU and 1,000 WCU)                                                        | No                    | Shard the partition key               | Hot keys cause `KeyRangeThroughputExceeded`            |
| Streams GetRecords                    | 2 calls per second per shard (not on the page checked)                                                     | Unverified            | n/a                                   | Check the Streams docs before relying on it            |

Sources: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html ; https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/throttling-key-range-limit-exceeded-mitigation.html

## SQS (standard)

| Limit                    | Default                                  | Adjustable               | Request                      | Why                                     |
| ------------------------ | ---------------------------------------- | ------------------------ | ---------------------------- | --------------------------------------- |
| API calls per action     | Nearly unlimited (no fixed number given) | n/a                      | n/a                          | Not a constraint                        |
| Message size             | 1 MiB                                    | No                       | n/a                          | Keep payloads small                     |
| SendMessageBatch entries | 10                                       | No                       | n/a                          | About 100k batch calls for 1M messages  |
| In-flight messages       | About 120,000 (approximate)              | Yes, via support request | Only if backlog exceeds 120k | Long polling avoids errors at the limit |

Sources: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html ; https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html

## ECS Fargate (sender service)

| Limit                            | Default                              | Adjustable | Request                            | Why                                                   |
| -------------------------------- | ------------------------------------ | ---------- | ---------------------------------- | ----------------------------------------------------- |
| On-Demand vCPU resource count    | 6 vCPUs per Region (code L-3032A538) | Yes        | At least 500 vCPUs                 | Assumes about 250 two-vCPU senders; sizing unverified |
| Fargate Spot vCPU resource count | 6 vCPUs per Region (code L-36FBB829) | Yes        | At least 500 vCPUs if Spot is used | Same reason                                           |
| RunTask / task launch bucket     | 100 burst, 20 per second refill      | Yes        | Pre-scale                          | Launch bucket for On-Demand tasks                     |
| Tasks launched per service       | 500 per minute (about 8.3/s)         | No         | Pre-scale before the send          | Autoscaling alone cannot keep up                      |
| Tasks per RunTask call           | 10                                   | No         | n/a                                | Batch launches                                        |

Sources: https://docs.aws.amazon.com/general/latest/gr/ecs-service.html ; https://docs.aws.amazon.com/AmazonECS/latest/developerguide/throttling.html

## Secrets Manager

| Limit                         | Default      | Adjustable | Request | Why                                    |
| ----------------------------- | ------------ | ---------- | ------- | -------------------------------------- |
| GetSecretValue                | 10,000 TPS   | No         | n/a     | Cache secrets in the sender and Lambda |
| PutSecretValue / CreateSecret | 50 TPS each  | No         | n/a     | Not on the hot path                    |
| Max secret size               | 65,536 bytes | No         | n/a     | Not a constraint                       |

Source: https://docs.aws.amazon.com/secretsmanager/latest/userguide/reference_limits.html

## SSM Parameter Store

| Limit                             | Default                                                   | Adjustable                                 | Request                   | Why                                          |
| --------------------------------- | --------------------------------------------------------- | ------------------------------------------ | ------------------------- | -------------------------------------------- |
| GetParameter, standard throughput | 40 TPS, shared with GetParameters and GetParametersByPath | No                                         | Turn on higher throughput | 40 TPS is far below any load in the path     |
| GetParameter, higher throughput   | 10,000 TPS (GetParameters 1,000; GetParametersByPath 100) | Service setting, not a Service Quotas item | Enable the setting        | Load parameters once at cold start and cache |

Source: https://docs.aws.amazon.com/systems-manager/latest/userguide/parameter-store-throughput.html

## Aurora Serverless v2 and RDS Data API

| Limit                  | Default                                  | Adjustable | Request                                  | Why                                                   |
| ---------------------- | ---------------------------------------- | ---------- | ---------------------------------------- | ----------------------------------------------------- |
| Capacity range         | 0 to 384 ACU (PostgreSQL), 0.5 ACU steps | Yes        | Set the max to the largest expected need | Keep check-ins out of Aurora on the hot path          |
| Data API response size | 1 MiB                                    | No         | n/a                                      | Page large results                                    |
| Data API rate limit    | Not published on the page checked        | Unverified | n/a                                      | Keep Data API out of the request path                 |
| Max connections        | Formula not on the page checked          | Unverified | n/a                                      | Use the Data API, not direct connections, from Lambda |

Sources: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2.requirements.html ; https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.limitations.html

## CloudWatch Logs

| Limit                                      | Default          | Adjustable            | Request            | Why                                                                           |
| ------------------------------------------ | ---------------- | --------------------- | ------------------ | ----------------------------------------------------------------------------- |
| PutLogEvents TPS (per account, per Region) | 5,000 per second | Yes (code L-7E1FAE88) | 10,000 or log less | One PutLogEvents per invocation at 50k invocations/s would be 10x the default |
| Batch size                                 | 1 MB             | No                    | n/a                | Not a constraint                                                              |
| Event size                                 | 1,024 KB         | No                    | n/a                | Not a constraint                                                              |

Source: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html

## S3

| Limit               | Default                                                          | Adjustable            | Request                          | Why                               |
| ------------------- | ---------------------------------------------------------------- | --------------------- | -------------------------------- | --------------------------------- |
| Requests per prefix | 3,500 PUT/COPY/POST/DELETE per second; 5,500 GET/HEAD per second | Not stated as a quota | Spread writes over many prefixes | Ten prefixes give about 35k PUT/s |

Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html

## AWS WAF

| Limit                           | Default   | Adjustable | Request               | Why                       |
| ------------------------------- | --------- | ---------- | --------------------- | ------------------------- |
| Requests per second per web ACL | 100,000   | Yes        | Confirm current value | Above the 50k peak        |
| Rate-based rules per web ACL    | 10        | No         | n/a                   | Plan rules within the cap |
| Web ACL capacity (WCU)          | 5,000 max | No         | n/a                   | Not a constraint          |

Source: https://docs.aws.amazon.com/waf/latest/developerguide/limits.html

## CloudFront

Not researched in this pass. Unverified.

## Apple APNs (token-based, HTTP/2)

| Limit                   | Default                                                             | Adjustable | Request                        | Why                                                                      |
| ----------------------- | ------------------------------------------------------------------- | ---------- | ------------------------------ | ------------------------------------------------------------------------ |
| Payload                 | 4 KB (VoIP 5 KB)                                                    | No         | n/a                            | Keep alert payloads small                                                |
| Provider token validity | 1 hour; `ExpiredProviderToken` (403) after that                     | No         | Refresh about every 50 minutes | Do not mint a new token per request (`TooManyProviderTokenUpdates`, 429) |
| Per-device throttling   | `TooManyRequests` (429) if too many requests go to one device token | No         | n/a                            | Dedupe sends                                                             |
| Concurrent streams      | Not fixed; do not assume a number                                   | No         | Several connections            | Keep connections open                                                    |
| Per-second total rate   | Not found; unverified                                               | Unverified | Ask Apple                      | 16.7k/s needs confirmation                                               |

Source: https://developer.apple.com/library/archive/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/CommunicatingwithAPNs.html

## Firebase Cloud Messaging (HTTP v1)

| Limit                                       | Default                                                         | Adjustable                             | Request                                                                     | Why                                                                         |
| ------------------------------------------- | --------------------------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Downstream messages per minute, per project | 600,000 (about 10,000/s)                                        | Yes, +25% standard; larger via support | Multiple projects (built, see below), or a support request for about 500k/s | 1M in 60 s needs 16.7k/s; 10M in 120 s needs 83k/s (about 8.3x the default) |
| Android per device                          | 240 messages per minute; 5,000 per hour                         | No                                     | n/a                                                                         | Avoid repeat sends                                                          |
| Collapsible messages per device             | Burst 20, then 1 per 3 min                                      | No                                     | n/a                                                                         | Use a collapse key sparingly                                                |
| Payload                                     | Unverified (commonly cited 4 KB)                                | Unverified                             | n/a                                                                         | Confirm                                                                     |
| Ramp guidance                               | Ramp from 0 to max over 60 s; avoid :00, :15, :30, :45 +/-2 min | n/a                                    | Follow it                                                                   | Spreads sends                                                               |

Sources: https://firebase.google.com/docs/cloud-messaging/throttling-and-quotas ; https://firebase.google.com/docs/cloud-messaging/scale-fcm

### Spreading Android over several Firebase projects (PERF-1)

A single project sends at most about 600,000 per minute, so 10M statewide takes 7 to 8 minutes. Relay spreads Android devices over N Firebase projects (`-c fcmProjectIds=...`, `FCM_PROJECT_IDS`) and limits each project with its own token bucket (`FCM_PROJECT_RATE_PER_SEC`, default 9,000 per second, 540,000 per minute, under the 600,000 default). A project that is at its rate makes only its own messages wait.

| Android recipients | Projects at the default quota and rate | Time to last Android push |
| ------------------ | -------------------------------------- | ------------------------- |
| 1M                 | 1                                      | about 1.9 min             |
| 1M                 | 2                                      | about 56 s                |
| 10M                | 1                                      | about 18.5 min            |
| 10M                | 8                                      | about 2.3 min             |
| 10M                | 16                                     | about 69 s                |

(Rows are recipients divided by N x 9,000 per second, with an even split; the hash split is within a few percent of even. iOS goes through APNs in parallel and is not limited by this.) A token only works with the Firebase project the app registered with, so the server assigns the project (stable hash of the device id), returns it as `fcmProject` from `POST /devices`, and the app must register with it; see `infra/README.md` (Sender stack) for the app's secondary `FirebaseApp` requirement. Only append project ids. Each project's quota can still be raised (support request) and the bucket rate raised with it.

## Top risks

1. **FCM project quota.** Default 600k messages per minute per project (about 10k/s) is below the 16.7k/s needed for 1M in 60 s, and statewide needs about 83k/s. Split Android sends across Firebase projects (built: see "Spreading Android over several Firebase projects") and start the quota increases early.
2. **Fargate vCPU and launch rate.** Default 6 vCPUs and 500 tasks per minute per service. Pre-scale senders with enough vCPUs before the send.
3. **API Gateway and Lambda concurrency.** API Gateway defaults to 10k RPS with a fixed 5k burst. Lambda defaults to 1,000 concurrent executions and ramps 1,000 environments per function per 10 s.
4. **DynamoDB on-demand table limit and warm-up.** 40k WRU per table by default. New tables start at 4k WRU, and the previous-peak rule throttles fast growth. Pre-warm and request about 120k.
5. **Lambda SQS scale-up.** Scale is 300 invokes per minute from 5, so reaching 1,000 takes about 3.3 min, longer than the window. Set `MaximumConcurrency` and pre-provision.

## Sources

- Lambda limits: https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html
- Lambda SQS scaling: https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-scaling.html
- API Gateway limits: https://docs.aws.amazon.com/apigateway/latest/developerguide/limits.html
- DynamoDB quotas: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html
- DynamoDB on-demand: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/on-demand-capacity-mode.html
- DynamoDB key-range throttling: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/throttling-key-range-limit-exceeded-mitigation.html
- SQS messages: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
- SQS queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html
- Fargate quotas: https://docs.aws.amazon.com/general/latest/gr/ecs-service.html
- Fargate throttling: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/throttling.html
- Secrets Manager: https://docs.aws.amazon.com/secretsmanager/latest/userguide/reference_limits.html
- SSM Parameter Store: https://docs.aws.amazon.com/systems-manager/latest/userguide/parameter-store-throughput.html
- Aurora Serverless v2: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2.requirements.html
- RDS Data API: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/data-api.limitations.html
- CloudWatch Logs: https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/cloudwatch_limits_cwl.html
- S3: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
- AWS WAF: https://docs.aws.amazon.com/waf/latest/developerguide/limits.html
- APNs: https://developer.apple.com/library/archive/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/CommunicatingwithAPNs.html
- FCM quotas: https://firebase.google.com/docs/cloud-messaging/throttling-and-quotas
- FCM scale: https://firebase.google.com/docs/cloud-messaging/scale-fcm
