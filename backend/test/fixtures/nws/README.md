# NWS alert test fixtures

Test fixtures for alert ingestion. Each file is a GeoJSON FeatureCollection shaped like the response from the NWS active-alerts endpoint (`GET https://api.weather.gov/alerts/active?area=CA`).

## Provenance

**No fixture here is a recording of a real NWS response.** Nothing was copied from api.weather.gov. The sandbox cannot reach that host, and the live fetch was refused by the site's robots rules. The field names and shapes follow the NWS API documentation (weather.gov/documentation/services-web-api). The values are constructed:

- Dates use the ISO 8601 form with a `-08:00` offset (Pacific Standard Time, November 9, 2026).
- Headlines, descriptions, instructions, polygons, and NWS office names are invented.
- UGC zone codes (for example `CAZ513`) and SAME codes (for example `006085` for Santa Clara County) are plausible but not checked against the NWS zone registry.
- The `@context` block is a short version of the NWS JSON-LD context.
- The `references` shape (`@id`, `identifier`, `sender`, `sent`) and the enum values (`status`, `messageType`, `severity`, `certainty`, `urgency`, `category`) follow the NWS documentation and CAP 1.2 terms.

The only real-world input was a third-party NWS API reference page (alex-gekas.github.io/nws-api-reference). It was used to check the field names and date format. Its example values were not used as fixture data.

**Action for AE-02:** once the S3 raw archive is deployed, replace these constructed files with recordings of real responses. Keep the file names so tests keep working. Re-check every test that asserts on specific text, IDs, or counts.

## Files

| File                      | Contents                                                                                                                                                                                                                            | Purpose                                                                          |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| `active-ca-mixed.json`    | 4 current `Actual` alerts: Flash Flood Warning (polygon, Santa Clara County), Heat Advisory (`geometry: null`, UGC only), Red Flag Warning (polygon), Winter Storm Watch (`geometry: null`).                                        | Happy path. Covers both geometry cases and several event types.                  |
| `update-chain.json`       | Original Flood Warning (`messageType: Alert`), then an Update (`messageType: Update`). The Update's `references` points to the original's `id` and `sent` time.                                                                     | Update handling. Ingestion should link the Update to the original.               |
| `expired-references.json` | Three alerts with the NWS per-feature `properties.parameters.expiredReferences` (strings `<sender>,<identifier>,<sent>`): one alert that expires another listed alert, and an Update that lists the message it replaces as expired. | Feed reconciliation (`expiredReferencesOf`). Only the first case ends an alert.  |
| `cancel.json`             | A Cancel (`messageType: Cancel`) for the Flood Warning in `update-chain.json`. Its `references` points to the Update's `id` and `sent` time, the latest message in the chain.                                                       | Cancel handling. Its target exists in `update-chain.json`.                       |
| `non-actual.json`         | One alert each with `status` `Test`, `Exercise`, and `System`.                                                                                                                                                                      | These must be rejected by ingestion. Only `Actual` is accepted.                  |
| `malformed.json`          | Four features, each breaking one rule: no `id` or `properties.id`; no `expires`; `sent` set to the invalid date `2026-13-45T99:99:00-08:00`; `severity` set to the unknown value `Catastrophic`.                                    | Validation. Each bad feature should be rejected without failing the whole batch. |
| `empty.json`              | A FeatureCollection with `features: []`.                                                                                                                                                                                            | Valid response with no active alerts. Must not be treated as an error.           |

## Consistency notes

- `update-chain.json` and `cancel.json` use the same URN family (`urn:oid:2.49.0.1.840.0.e2b7a4.001.N`). The Cancel's `references` value matches the Update's `id` and `sent` exactly.
- The Cancel refers to the Update, not the original Flood Warning, because the Update is the latest message in the chain. A test that expects the Cancel to point to the original would need the reference changed.

## Regenerating

These files were generated by a script, not edited by hand. Change the generator and run it again rather than editing the JSON. The generator is not in this repository.
