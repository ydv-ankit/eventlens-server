import logger from "@/shared/utils/logger";
import { ENV } from "@/shared/utils/env";
import { opentelemetrySDK } from "@/shared/lib/instrumentation";

const PORT = Number(ENV.APP_PORT || 8080);
const HOST = "0.0.0.0";

async function bootstrap() {
  try {
    opentelemetrySDK.start();

    const [{ app }, { seedDatabase }, { connectRedis }] =
      await Promise.all([
        import("@/api/app"),
        import("@/shared/utils/db/seed-db"),
        import("@/shared/lib/config/redis"),
      ]);

    await seedDatabase();
    await connectRedis();

    app.listen(PORT, HOST, 4096, () => {
      logger.debug(`⚙️ Server listening on port: ${PORT}`);
    });

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
