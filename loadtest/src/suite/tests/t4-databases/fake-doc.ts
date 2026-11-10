/**
 * A small in-memory stand-in for a DynamoDBDocumentClient, used ONLY where Docker is not available
 * (the vitest smoke run of this test). It implements exactly the commands and expression forms the
 * backend adapters use, with DynamoDB's semantics for them (conditional writes, key order, GSIs,
 * paging with LastEvaluatedKey, transactions with CancellationReasons). It is not a general
 * DynamoDB emulator: an unsupported expression throws, so a drift in the adapters shows up as a
 * failing smoke test rather than silently passing.
 */
import {
  ConditionalCheckFailedException,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb';
import { TABLE_SPECS, type TableNames, type TableSpec } from './tables.js';

type Item = Record<string, unknown>;
type Names = Record<string, string>;
type Values = Record<string, unknown>;
interface CommandLike {
  constructor: { name: string };
  input: Record<string, unknown>;
}

const clone = <T>(v: T): T => structuredClone(v);

// ---------- condition expressions ----------

function tokenize(expr: string): string[] {
  return expr.match(/attribute_not_exists|attribute_exists|<>|[()=<>]|[#:]?\w+/g) ?? [];
}

function evalCondition(
  expr: string,
  item: Item | undefined,
  names: Names,
  values: Values,
): boolean {
  const toks = tokenize(expr);
  let i = 0;
  const attr = (t: string): string => names[t] ?? t;
  const operand = (t: string): unknown => (t.startsWith(':') ? values[t] : item?.[attr(t)]);
  const parseOr = (): boolean => {
    let v = parseAnd();
    while (toks[i] === 'OR') {
      i++;
      const r = parseAnd();
      v = v || r;
    }
    return v;
  };
  const parseAnd = (): boolean => {
    let v = parseTerm();
    while (toks[i] === 'AND') {
      i++;
      const r = parseTerm();
      v = v && r;
    }
    return v;
  };
  const parseTerm = (): boolean => {
    const t = toks[i++];
    if (t === '(') {
      const v = parseOr();
      if (toks[i++] !== ')') throw new Error(`fake-doc: bad condition: ${expr}`);
      return v;
    }
    if (t === 'attribute_not_exists' || t === 'attribute_exists') {
      i++; // (
      const name = attr(toks[i++] as string);
      i++; // )
      const has = item !== undefined && item[name] !== undefined;
      return t === 'attribute_exists' ? has : !has;
    }
    const op = toks[i++];
    const left = operand(t as string);
    const right = operand(toks[i++] as string);
    if (left === undefined || right === undefined) return false;
    switch (op) {
      case '=':
        return left === right;
      case '<>':
        return left !== right;
      case '<':
        return (left as number) < (right as number);
      case '>':
        return (left as number) > (right as number);
      default:
        throw new Error(`fake-doc: unsupported operator ${String(op)} in: ${expr}`);
    }
  };
  const out = parseOr();
  if (i !== toks.length) throw new Error(`fake-doc: trailing tokens in condition: ${expr}`);
  return out;
}

// ---------- update expressions ----------

function applyUpdate(item: Item, expr: string, names: Names, values: Values): void {
  const clauses = expr.split(/\b(SET|REMOVE|ADD)\b/).map((s) => s.trim());
  for (let c = 1; c < clauses.length; c += 2) {
    const kind = clauses[c];
    const body = clauses[c + 1] ?? '';
    const attr = (t: string): string => names[t] ?? t;
    if (kind === 'SET') {
      for (const part of body.split(/,(?![^(]*\))/)) {
        const [lhs, rhs] = part.split('=').map((s) => s.trim()) as [string, string];
        const m = /^if_not_exists\(([^,]+),\s*([^)]+)\)$/.exec(rhs);
        if (m) {
          const name = attr((m[1] as string).trim());
          if (item[name] === undefined) item[name] = clone(values[(m[2] as string).trim()]);
        } else item[attr(lhs)] = clone(values[rhs]);
      }
    } else if (kind === 'REMOVE') {
      for (const t of body.split(',')) delete item[attr(t.trim())];
    } else if (kind === 'ADD') {
      for (const part of body.split(',')) {
        const [name, v] = part.trim().split(/\s+/) as [string, string];
        item[attr(name)] = ((item[attr(name)] as number | undefined) ?? 0) + (values[v] as number);
      }
    }
  }
}

// ---------- the client ----------

export class FakeDoc {
  private readonly tables = new Map<string, { spec: TableSpec; rows: Map<string, Item> }>();

  constructor(names: TableNames) {
    for (const spec of TABLE_SPECS) this.tables.set(names[spec.key], { spec, rows: new Map() });
  }

  private table(name: unknown): { spec: TableSpec; rows: Map<string, Item> } {
    const t = this.tables.get(String(name));
    if (!t) throw new Error(`fake-doc: unknown table ${String(name)}`);
    return t;
  }

  private keyOf(spec: TableSpec, item: Item): string {
    const h = item[spec.hash.name];
    const r = spec.range ? item[spec.range.name] : '';
    if (typeof h !== 'string' || (spec.range && typeof r !== 'string')) {
      throw new Error(`fake-doc: bad key for ${spec.key}`);
    }
    return `${h}\u0000${String(r)}`;
  }

  /** Number of items in a table (test helper). */
  count(name: string): number {
    return this.table(name).rows.size;
  }

  private checkCondition(input: Record<string, unknown>, existing: Item | undefined): boolean {
    const cond = input['ConditionExpression'] as string | undefined;
    if (!cond) return true;
    return evalCondition(
      cond,
      existing,
      (input['ExpressionAttributeNames'] ?? {}) as Names,
      (input['ExpressionAttributeValues'] ?? {}) as Values,
    );
  }

  private put(input: Record<string, unknown>): boolean {
    const t = this.table(input['TableName']);
    const item = clone(input['Item'] as Item);
    const key = this.keyOf(t.spec, item);
    if (!this.checkCondition(input, t.rows.get(key))) return false;
    t.rows.set(key, item);
    return true;
  }

  private update(input: Record<string, unknown>): boolean {
    const t = this.table(input['TableName']);
    const keyItem = input['Key'] as Item;
    const key = this.keyOf(t.spec, keyItem);
    const existing = t.rows.get(key);
    if (!this.checkCondition(input, existing)) return false;
    const next = existing ? clone(existing) : clone(keyItem);
    applyUpdate(
      next,
      input['UpdateExpression'] as string,
      (input['ExpressionAttributeNames'] ?? {}) as Names,
      (input['ExpressionAttributeValues'] ?? {}) as Values,
    );
    t.rows.set(key, next);
    return true;
  }

  async send(command: unknown): Promise<unknown> {
    const { constructor: ctor, input } = command as CommandLike;
    switch (ctor.name) {
      case 'PutCommand':
        if (!this.put(input)) throw cond();
        return {};
      case 'GetCommand': {
        const t = this.table(input['TableName']);
        const item = t.rows.get(this.keyOf(t.spec, input['Key'] as Item));
        return item ? { Item: clone(item) } : {};
      }
      case 'DeleteCommand': {
        const t = this.table(input['TableName']);
        const key = this.keyOf(t.spec, input['Key'] as Item);
        if (!this.checkCondition(input, t.rows.get(key))) throw cond();
        t.rows.delete(key);
        return {};
      }
      case 'UpdateCommand':
        if (!this.update(input)) throw cond();
        return {};
      case 'BatchWriteCommand': {
        const req = input['RequestItems'] as Record<
          string,
          Array<{ PutRequest?: { Item: Item }; DeleteRequest?: { Key: Item } }>
        >;
        for (const [name, ops] of Object.entries(req)) {
          const t = this.table(name);
          if (ops.length > 25) throw new Error('fake-doc: more than 25 requests in a batch');
          for (const op of ops) {
            if (op.PutRequest) {
              const item = clone(op.PutRequest.Item);
              t.rows.set(this.keyOf(t.spec, item), item);
            } else if (op.DeleteRequest) {
              t.rows.delete(this.keyOf(t.spec, op.DeleteRequest.Key));
            }
          }
        }
        return { UnprocessedItems: {} };
      }
      case 'TransactWriteCommand':
        return this.transact(input['TransactItems'] as Array<Record<string, unknown>>);
      case 'QueryCommand':
        return this.query(input);
      case 'ScanCommand':
        return this.scan(input);
      default:
        throw new Error(`fake-doc: unsupported command ${ctor.name}`);
    }
  }

  private transact(items: Array<Record<string, unknown>>): unknown {
    const reasons: Array<{ Code: string }> = [];
    let failed = false;
    for (const it of items) {
      const op = (it['Put'] ?? it['Update']) as Record<string, unknown>;
      const t = this.table(op['TableName']);
      const keyItem = (op['Item'] ?? op['Key']) as Item;
      const ok = this.checkCondition(op, t.rows.get(this.keyOf(t.spec, keyItem)));
      reasons.push({ Code: ok ? 'None' : 'ConditionalCheckFailed' });
      if (!ok) failed = true;
    }
    if (failed) {
      throw new TransactionCanceledException({
        message: 'Transaction cancelled',
        $metadata: {},
        CancellationReasons: reasons,
      });
    }
    for (const it of items) {
      if (it['Put']) this.put({ ...(it['Put'] as object), ConditionExpression: undefined });
      else this.update({ ...(it['Update'] as object), ConditionExpression: undefined });
    }
    return {};
  }

  private page(
    spec: TableSpec,
    all: Item[],
    startKey: Item | undefined,
    limit: number | undefined,
    indexHash: string | undefined,
  ): { items: Item[]; last?: Item } {
    let from = 0;
    if (startKey) {
      const k = this.keyOf(spec, startKey);
      const idx = all.findIndex((it) => this.keyOf(spec, it) === k);
      from = idx < 0 ? all.length : idx + 1;
    }
    const slice = limit === undefined ? all.slice(from) : all.slice(from, from + limit);
    const more = limit !== undefined && from + limit < all.length;
    const lastItem = slice[slice.length - 1];
    const last = more && lastItem ? this.keyFields(spec, lastItem, indexHash) : undefined;
    return { items: slice.map(clone), ...(last ? { last } : {}) };
  }

  private keyFields(spec: TableSpec, item: Item, indexHash?: string): Item {
    const k: Item = { [spec.hash.name]: item[spec.hash.name] };
    if (spec.range) k[spec.range.name] = item[spec.range.name];
    if (indexHash) k[indexHash] = item[indexHash];
    return k;
  }

  private sortedRows(spec: TableSpec, rows: Map<string, Item>): Item[] {
    return [...rows.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([, v]) => v);
  }

  private query(input: Record<string, unknown>): unknown {
    const t = this.table(input['TableName']);
    const names = (input['ExpressionAttributeNames'] ?? {}) as Names;
    const values = (input['ExpressionAttributeValues'] ?? {}) as Values;
    const m = /^\s*([#\w]+)\s*=\s*(:\w+)\s*$/.exec(input['KeyConditionExpression'] as string);
    if (!m) throw new Error('fake-doc: unsupported KeyConditionExpression');
    const attrName = names[m[1] as string] ?? (m[1] as string);
    const want = values[m[2] as string];
    const indexName = input['IndexName'] as string | undefined;
    if (indexName && !t.spec.gsis.some((g) => g.name === indexName && g.hash === attrName)) {
      throw new Error(`fake-doc: index ${indexName} does not match ${attrName}`);
    }
    if (!indexName && attrName !== t.spec.hash.name) throw new Error('fake-doc: not the hash key');
    const all = this.sortedRows(t.spec, t.rows).filter((it) => it[attrName] === want);
    const { items, last } = this.page(
      t.spec,
      all,
      input['ExclusiveStartKey'] as Item | undefined,
      input['Limit'] as number | undefined,
      indexName ? attrName : undefined,
    );
    return { Items: items, Count: items.length, ...(last ? { LastEvaluatedKey: last } : {}) };
  }

  private scan(input: Record<string, unknown>): unknown {
    const t = this.table(input['TableName']);
    const all = this.sortedRows(t.spec, t.rows);
    const { items, last } = this.page(
      t.spec,
      all,
      input['ExclusiveStartKey'] as Item | undefined,
      input['Limit'] as number | undefined,
      undefined,
    );
    return {
      Count: items.length,
      ...(input['Select'] === 'COUNT' ? {} : { Items: items }),
      ...(last ? { LastEvaluatedKey: last } : {}),
    };
  }
}

function cond(): ConditionalCheckFailedException {
  return new ConditionalCheckFailedException({
    message: 'The conditional request failed',
    $metadata: {},
  });
}
