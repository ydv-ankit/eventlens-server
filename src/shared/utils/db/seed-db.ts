import db from "@/shared/lib/config/db";
import logger from "../logger";

export const seedDatabase = async () => {
  try {
    // CREATE SCHEMAS
    await db.query(`
            CREATE SCHEMA IF NOT EXISTS analytics
        `);

    // CREATE SEQUENCES
    await db.query(`
            CREATE SEQUENCE IF NOT EXISTS analytics.project_seq
            START WITH 1
            NO CYCLE
            CACHE 1
            INCREMENT BY 1
        `);

    await db.query(`
            CREATE SEQUENCE IF NOT EXISTS analytics.user_seq
            START WITH 1
            NO CYCLE
            CACHE 1
            INCREMENT BY 1
        `);

    await db.query(`
            CREATE SEQUENCE IF NOT EXISTS analytics.raw_event_seq
            START WITH 1
            NO CYCLE
            CACHE 1
            INCREMENT BY 1
        `);

    // CREATE TABLES
    // project
    await db.query(`
            CREATE TABLE IF NOT EXISTS analytics.project (
                id              INT             PRIMARY KEY DEFAULT nextval('analytics.project_seq'),
                name            VARCHAR(50)     NOT NULL,
                api_key         VARCHAR(40),
                user_id         INT             NOT NULL,
                created_at      TIMESTAMP       DEFAULT     now()
            );
        `);

    // user
    await db.query(`
            CREATE TABLE IF NOT EXISTS analytics.user (
                id              INT             PRIMARY KEY DEFAULT nextval('analytics.user_seq'),
                name            VARCHAR(50)     NOT NULL,
                created_at      TIMESTAMP       DEFAULT     now()
            );
        `);

    // raw_event
    await db.query(`
            CREATE TABLE IF NOT EXISTS analytics.raw_event (
                id              INT         PRIMARY KEY DEFAULT nextval('analytics.raw_event_seq'),
                event_name      TEXT        NOT NULL,
                metadata        JSONB       NOT NULL,
                project_id      INT         NOT NULL,
                user_id         TEXT,
                timestamp       TIMESTAMP   DEFAULT     now(),
                created_at      TIMESTAMP       DEFAULT     now()
            );
        `);

    // ADD CONSTRAINTS
    // project table
    await db.query(`
            ALTER TABLE analytics.project
            DROP CONSTRAINT IF EXISTS fk_analytics_project;

            ALTER TABLE analytics.project
            ADD CONSTRAINT fk_analytics_project
            FOREIGN KEY (user_id)
            REFERENCES analytics.user(id);
        `);

    // raw_event table
    await db.query(`
            ALTER TABLE analytics.raw_event
            DROP CONSTRAINT IF EXISTS fk_analytics_raw_event;

            ALTER TABLE analytics.raw_event
            ADD CONSTRAINT fk_analytics_raw_event
            FOREIGN KEY (project_id)
            REFERENCES analytics.project(id);
        `);

    logger.debug("database seeding completed");
  } catch (error) {
    logger.error("failed seeding database, db operations may fail -> " + error);
  }
};
