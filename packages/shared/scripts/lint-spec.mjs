// Validates openapi/relay.yaml against the OpenAPI 3.1 schema and resolves all $refs.
import SwaggerParser from '@apidevtools/swagger-parser';
import { fileURLToPath } from 'node:url';

try {
  const path = fileURLToPath(new URL('../openapi/relay.yaml', import.meta.url));
  const api = await SwaggerParser.validate(path);
  console.log(`OpenAPI spec OK: ${api.info.title} ${api.info.version}`);
} catch (err) {
  console.error(`OpenAPI spec invalid: ${err.message}`);
  process.exit(1);
}
