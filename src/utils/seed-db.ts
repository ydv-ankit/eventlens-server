import db from "@/lib/config/db"
import logger from "./logger";

export const seedDatabase = async () => {
    try {
        // CREATE SCHEMAS
        await db.query(`
            CREATE SCHEMA IF NOT EXISTS analytics
        `)

        // CREATE SEQUENCES
        await db.query(`
            CREATE SEQUENCE IF NOT EXISTS analytics.project_seq
            START WITH 1
            NO CYCLE
            CACHE 1
            INCREMENT BY 1
        `)

        await db.query(`
            CREATE SEQUENCE IF NOT EXISTS analytics.user_seq
            START WITH 1
            NO CYCLE
            CACHE 1
            INCREMENT BY 1
        `)

        // CREATE TABLES
        // project
        await db.query(`
            CREATE TABLE IF NOT EXISTS analytics.project (
                id          INT          PRIMARY KEY DEFAULT nextval('analytics.project_seq'),
                name        VARCHAR(50)  NOT NULL,
                apikey      VARCHAR(40),
                userid      INT
            );
        `);

        // user
        await db.query(`
            CREATE TABLE IF NOT EXISTS analytics.user (
                id      INT            PRIMARY KEY DEFAULT nextval('analytics.user_seq'),
                name    VARCHAR(50)    NOT NULL
            );
        `)

        // ADD CONSTRAINTS
        await db.query(`
            ALTER TABLE analytics.project
            DROP CONSTRAINT IF EXISTS fk_analytics_project;

            ALTER TABLE analytics.project
            ADD CONSTRAINT fk_analytics_project
            FOREIGN KEY (userid)
            REFERENCES analytics.user(id);
        `)

        logger.info("database seeding completed");
    } catch (error) {
        logger.error("failed seeding database, db operations may fail" + error);
    }
}