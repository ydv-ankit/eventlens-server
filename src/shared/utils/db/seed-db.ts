import db from "@/shared/lib/config/db";
import logger from "../logger";

export const seedDatabase = async () => {
  try {
    await db.query(`CREATE SCHEMA IF NOT EXISTS analytics`);

    await db.query(`
      CREATE SEQUENCE IF NOT EXISTS analytics.project_seq
      START WITH 1 NO CYCLE CACHE 1 INCREMENT BY 1
    `);

    await db.query(`
      CREATE SEQUENCE IF NOT EXISTS analytics.api_key_seq
      START WITH 1 NO CYCLE CACHE 1 INCREMENT BY 1
    `);

    await db.query(`
      CREATE SEQUENCE IF NOT EXISTS analytics.raw_event_seq
      START WITH 1 NO CYCLE CACHE 1 INCREMENT BY 1
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS analytics.user (
        id             SERIAL      PRIMARY KEY,
        clerk_user_id  TEXT        UNIQUE NOT NULL,
        email          TEXT        NOT NULL DEFAULT '',
        name           TEXT        NOT NULL DEFAULT '',
        created_at     TIMESTAMPTZ DEFAULT now()
      )
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS analytics.project (
        id             INT         PRIMARY KEY DEFAULT nextval('analytics.project_seq'),
        name           VARCHAR(50) NOT NULL,
        description    TEXT,
        clerk_user_id  TEXT        NOT NULL DEFAULT '',
        created_at     TIMESTAMP   DEFAULT now()
      )
    `);

    await db.query(`
      ALTER TABLE analytics.project
      ADD COLUMN IF NOT EXISTS clerk_user_id TEXT NOT NULL DEFAULT ''
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS analytics.api_key (
        id           INT         PRIMARY KEY DEFAULT nextval('analytics.api_key_seq'),
        project_id   INT         NOT NULL,
        key_hash     VARCHAR(64) NOT NULL,
        last_used_at TIMESTAMP,
        is_active    BOOLEAN     NOT NULL DEFAULT true,
        created_at   TIMESTAMP   DEFAULT now()
      )
    `);

    await db.query(`
      CREATE TABLE IF NOT EXISTS analytics.raw_event (
        id          INT         PRIMARY KEY DEFAULT nextval('analytics.raw_event_seq'),
        event_name  TEXT        NOT NULL,
        metadata    JSONB       NOT NULL,
        project_id  INT         NOT NULL,
        user_id     TEXT,
        timestamp   TIMESTAMP   DEFAULT now(),
        created_at  TIMESTAMP   DEFAULT now()
      )
    `);

    await db.query(`
      ALTER TABLE analytics.api_key
      DROP CONSTRAINT IF EXISTS fk_api_key_project;

      ALTER TABLE analytics.api_key
      ADD CONSTRAINT fk_api_key_project
      FOREIGN KEY (project_id) REFERENCES analytics.project(id) ON DELETE CASCADE
    `);

    await db.query(`
      ALTER TABLE analytics.raw_event
      ADD COLUMN IF NOT EXISTS session_id TEXT
    `);

    await db.query(`
      ALTER TABLE analytics.raw_event
      DROP CONSTRAINT IF EXISTS fk_analytics_raw_event;

      ALTER TABLE analytics.raw_event
      ADD CONSTRAINT fk_analytics_raw_event
      FOREIGN KEY (project_id) REFERENCES analytics.project(id)
    `);

    logger.debug("database seeding completed");
  } catch (error) {
    logger.error("failed seeding database, db operations may fail -> " + error);
  }
};
