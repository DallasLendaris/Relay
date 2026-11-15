import { createHash } from 'node:crypto';
import {
  PublicAlertDocumentSchema,
  type Alert,
  type AlertPointer,
  type PublicAlertDocument,
} from '@relay/shared';
import { publicAlertDocKey } from './paths.js';

/** Hex SHA-256 of the alert's cell ids, sorted, joined with "\n". */
export function cellsDigest(cells: readonly string[]): string {
  return createHash('sha256')
    .update([...cells].sort().join('\n'))
    .digest('hex');
}

/**
 * The cell-free document the app reads. `signature` is the alert's own signature (made by the
 * existing alert signer over the full alert), so this does not touch the signing primitives; the
 * cell count and digest bind the document to the signed cells.
 */
export function buildPublicDocument(a: Alert): PublicAlertDocument {
  const { area, ...rest } = a;
  return PublicAlertDocumentSchema.parse({
    ...rest,
    version: 1,
    area: {
      description: area.description,
      cellCount: new Set(area.h3Cells).size,
      cellsSha256: cellsDigest([...new Set(area.h3Cells)]),
    },
  });
}

/** True when `doc` is exactly what buildPublicDocument makes of the full (verified) alert. */
export function documentMatchesAlert(doc: PublicAlertDocument, full: Alert): boolean {
  return JSON.stringify(doc) === JSON.stringify(buildPublicDocument(full));
}

export function buildPointer(a: Alert): AlertPointer {
  return {
    version: 1,
    alertId: a.id,
    sourceId: a.sourceId,
    state: a.state,
    expires: a.expires,
    doc: `/${publicAlertDocKey(a)}`,
  };
}
