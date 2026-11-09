# Relay

Relay gets official emergency alerts to people in plain language and their own language, confirms they received them, and lets families see that everyone is okay. When a government agency adopts it, Relay also shows where residents aren't responding and lets the agency send its own local alerts.

SJSU Software Engineering senior capstone (CMPE 195A/B), 2026–2027.

## Repo layout

| Folder                | Area           | What lives here                                                                                       |
| --------------------- | -------------- | ----------------------------------------------------------------------------------------------------- |
| `packages/shared`     | Shared         | `@relay/shared`: shared formats (alert, check-in) and the OpenAPI contract                            |
| `backend/`            | Backend        | `@relay/backend`: Lambda handlers and engine logic (ingest, match, deliver, check-in)                 |
| `backend/migrations/` | Backend        | SQL migrations for Aurora PostgreSQL + PostGIS, run from CI, never by hand in the AWS console         |
| `infra/`              | Infrastructure | `@relay/infra`: AWS CDK (TypeScript) app for Lambda, DynamoDB, Aurora Serverless v2, SQS, API Gateway |
| `app/`                | Public app     | React Native app: places, alerts, check-ins, family circle                                            |
| `agency/`             | Agency side    | React (Vite) SPA: response-cluster map, local alert tool, agency accounts                             |
| `docs/`               | Shared         | Design notes and team docs                                                                            |

## Development

Requires Node 22. From the repo root:

```
npm ci
npm run lint && npm run typecheck && npm test
```

## How we work

- All work is tracked as issues on the project board: **To Do → In Progress → In Review → Done**.
- Every issue has a **Done when** line, an area label, and an assignee.
- Work happens on a branch named `<issue-number>-short-description`, for example `12-nws-ingest`.
- Every pull request needs one approval from someone outside that area, and should say `Closes #<issue>` so the issue closes when it merges.
- Changes to the shared formats in `docs/` need all three of us to agree.
- Never commit API keys, passwords, or `.env` files.

Full team structure: see "Relay Team Structure" in the Sr Design Project Drive folder.

## Team

- Dallas Lendaris
- Steven Tinoco
- Roman Lozano
