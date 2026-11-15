/**
 * Small-cell suppression for agency check-in totals.
 *
 * DEFAULT PENDING THE TEAM'S PRIVACY-FLOOR DECISION (docs/agency-auth.md, "Decisions"): any count
 * below `threshold` (default DEFAULT_MIN_CELL_COUNT, env AGENCY_MIN_CELL_COUNT) is withheld, so a
 * small number of people in one H3 cell cannot be singled out.
 *
 * Withholding single cells is not enough when the area total is also shown: total minus the
 * visible cells would give the withheld value back. So suppression is two-step:
 *  1. primary: every cell below the threshold is withheld;
 *  2. complementary: while the withheld cells add up to less than the threshold (so the
 *     difference would reveal a small count), the smallest visible cell is withheld too.
 * An area whose total is below the threshold is withheld whole (total and every cell).
 *
 * Known limit: comparing two responses taken at different times can still reveal how a small
 * cell changed. Rounding or noise would close that; it is part of the open decision.
 */
export const DEFAULT_MIN_CELL_COUNT = 10;

export interface CellCount {
  h3Cell: string;
  checkedIn: number;
}

export interface ReportedCell {
  h3Cell: string;
  /** null when withheld. */
  checkedIn: number | null;
  suppressed: boolean;
}

export interface ReportedArea {
  checkedIn: number | null;
  suppressed: boolean;
  cells: ReportedCell[];
}

export function suppressArea(cells: readonly CellCount[], threshold: number): ReportedArea {
  if (!Number.isInteger(threshold) || threshold < 1) {
    throw new Error(`suppression threshold must be a positive integer, got ${threshold}`);
  }
  const sorted = [...cells].sort((a, b) => a.h3Cell.localeCompare(b.h3Cell));
  const total = sorted.reduce((sum, c) => sum + c.checkedIn, 0);
  if (total < threshold) {
    return {
      checkedIn: null,
      suppressed: true,
      cells: sorted.map((c) => ({ h3Cell: c.h3Cell, checkedIn: null, suppressed: true })),
    };
  }

  const hidden = new Set(sorted.filter((c) => c.checkedIn < threshold).map((c) => c.h3Cell));
  let residual = sorted.filter((c) => hidden.has(c.h3Cell)).reduce((s, c) => s + c.checkedIn, 0);
  // Smallest first, ties by cell id, so the result is deterministic.
  const visible = sorted
    .filter((c) => !hidden.has(c.h3Cell))
    .sort((a, b) => a.checkedIn - b.checkedIn || a.h3Cell.localeCompare(b.h3Cell));
  while (hidden.size > 0 && residual < threshold && visible.length > 0) {
    const next = visible.shift() as CellCount;
    hidden.add(next.h3Cell);
    residual += next.checkedIn;
  }

  return {
    checkedIn: total,
    suppressed: false,
    cells: sorted.map((c) =>
      hidden.has(c.h3Cell)
        ? { h3Cell: c.h3Cell, checkedIn: null, suppressed: true }
        : { h3Cell: c.h3Cell, checkedIn: c.checkedIn, suppressed: false },
    ),
  };
}
