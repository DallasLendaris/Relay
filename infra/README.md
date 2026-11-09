# @relay/infra

AWS CDK app (TypeScript) for Relay, deployed to us-west-2. The account comes from `CDK_DEFAULT_ACCOUNT`; nothing is hard-coded.

## What `IngestStack` creates

- S3 bucket for raw NWS responses: private, SSL-only, S3-managed encryption, moves to Infrequent Access after 30 days. Retained on prod, auto-deleted on dev.
- DynamoDB table `alerts-state` (pk `sourceId`), on-demand, point-in-time recovery on prod. Dedupe state only; the full alert record goes to Aurora later (ticket S-07).
- Ingest Lambda (`backend/src/ingest/handler.ts`, Node 22, ARM64, 70 s, 256 MB, reserved concurrency 1) with write access to the bucket and read/write on the table only.
- EventBridge rule `rate(1 minute)` with no retries (the next run catches up).
- CloudWatch alarm: Lambda Errors >= 3 in 5 minutes.
- Everything tagged `project=relay`, `stage=<stage>`.

## Commands

    npm install
    npx cdk synth -c stage=dev     # works offline (bundles with esbuild)
    npm test                       # assertion tests
    npm run typecheck

`stage` is `dev` (default) or `prod`.

## Deploys

Deploys will run from GitHub Actions using OIDC (ticket S-06, not done yet). Never deploy from a laptop with stored access keys.
