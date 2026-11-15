# @relay/shared

Shared formats and the API contract for Relay. Backend, app, and agency console all depend on this package.

## Files

| File                    | Purpose                                                                    |
| ----------------------- | -------------------------------------------------------------------------- |
| `openapi/relay.yaml`    | OpenAPI 3.1 spec. **Source of truth** for the API.                         |
| `src/generated/api.ts`  | TypeScript types generated from the spec. Committed. Never edit by hand.   |
| `src/alert.ts`          | `AlertSchema` (zod), `relaySeverityFor()`, `H3_RESOLUTION` (= 7).          |
| `src/checkin.ts`        | `CheckInSchema` (zod).                                                     |
| `src/index.ts`          | Public exports.                                                            |
| `scripts/lint-spec.mjs` | Validates the spec (`npm run lint`).                                       |
| `test/`                 | Schema examples, `relaySeverityFor`, spec validation, and the drift check. |

Zod gives runtime validation; the generated types give the API shape. `test/contract.test.ts`
proves each zod-inferred type (`Alert`, `CheckIn`) is assignable to the matching OpenAPI
component and vice versa, so they cannot drift. The check runs in `npm run typecheck`.

## Changing a format

1. Edit `openapi/relay.yaml`.
2. Run `npm run generate` and commit the updated `src/generated/api.ts`.
3. Update the zod schema in `src/alert.ts` or `src/checkin.ts` until `npm run typecheck` passes.
4. Run `npm run lint` and `npm test`.

## Commands

```
npm install
npm run generate    # regenerate src/generated/api.ts
npm run lint        # validate the OpenAPI spec
npm run typecheck
npm test
npm run build       # emits dist/
```

## Rule: format changes need all three teammates

Any change to the Alert format, the CheckIn format, or the API contract must be
approved by all three teammates before it merges. Other code depends on these
shapes, and a silent change can break delivery, check-in, or the agency view.
