import { app } from "@/app";
import logger from "@/utils/logger";
import { seedDatabase } from "./utils/seed-db";

const PORT = Number(process.env.PORT || 8080);

seedDatabase().then(() => app.listen(PORT, () => {
    logger.info(`⚙️ Server listening on port: ${PORT}`);
}))

process.on("unhandledRejection", (reason) => {
    logger.error("Unhandled rejection: " + reason);
});