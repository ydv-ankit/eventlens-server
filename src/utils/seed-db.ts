import db from "@/lib/config/db"
import logger from "./logger";

export const seedDatabase = async () => {
    try {
        // create tables
        await db.query("create table project (id uuid, name varchar(50))");
        logger.info("table created: project")
    } catch (error) {
        logger.error("error seeding database")
    }
}