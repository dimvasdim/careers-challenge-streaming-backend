import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import pg from 'pg';

// Recreates a dedicated `teton_test` database on the compose Postgres and
// applies db/schema.sql, so integration tests never touch dev data.
const ADMIN_URL = process.env.TEST_ADMIN_URL ?? 'postgres://teton:teton@localhost:5433/postgres';
export const TEST_DB = 'teton_test';

export default async function setup(): Promise<void> {
  const admin = new pg.Client({ connectionString: ADMIN_URL });
  await admin.connect();
  await admin.query(`DROP DATABASE IF EXISTS ${TEST_DB} WITH (FORCE)`);
  await admin.query(`CREATE DATABASE ${TEST_DB}`);
  await admin.end();

  const schema = readFileSync(join(import.meta.dirname, '..', '..', 'db', 'schema.sql'), 'utf8');
  const db = new pg.Client({ connectionString: ADMIN_URL.replace(/\/[^/]*$/, `/${TEST_DB}`) });
  await db.connect();
  await db.query(schema);
  await db.end();
}
