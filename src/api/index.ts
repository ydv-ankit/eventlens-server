import logger from "@/shared/utils/logger";
import { ENV } from "@/shared/utils/env";
import { opentelemetrySDK } from "@/shared/lib/instrumentation";

const PORT = Number(ENV.APP_PORT || 8080);
const HOST = "0.0.0.0";

async function bootstrap() {
  try {
    opentelemetrySDK.start();

    const [{ app }, { seedDatabase }, { connectRedis }, { startWorkers }] =
      await Promise.all([
        import("@/api/app"),
        import("@/shared/utils/db/seed-db"),
        import("@/shared/lib/config/redis"),
        import("@/worker"),
      ]);

    await seedDatabase();

    app.listen(PORT, HOST, 4096, () => {
      logger.info(`⚙️ Server listening on port: ${PORT}`);
    });

    await connectRedis();
    await startWorkers();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.error("Server bootstrap failed: " + message);
    process.exit(1);
  }
}

void bootstrap();

process.on("unhandledRejection", (reason) => {
  logger.error("Unhandled rejection: " + String(reason));
});
