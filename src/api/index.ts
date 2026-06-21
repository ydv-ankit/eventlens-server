import logger from "@/shared/utils/logger";
import { ENV } from "@/shared/utils/env";
import { opentelemetrySDK } from "@/shared/lib/instrumentation";
import { connectKafkaProducer, ensureKafkaTopics } from "@/shared/lib/config/kafka";
import { createServer } from "http";

const PORT = Number(ENV.APP_PORT || 8080);
const HOST = "0.0.0.0";

async function bootstrap() {
  try {
    opentelemetrySDK.start();

    const [{ app }, { seedDatabase }, { createWebSocketServer }] =
      await Promise.all([
        import("@/api/app"),
        import("@/shared/utils/db/seed-db"),
        import("@/api/ws"),
      ]);

    await seedDatabase();
    await ensureKafkaTopics();

    const httpServer = createServer(app);
    createWebSocketServer(httpServer);

    httpServer.listen(PORT, HOST, () => {
      logger.debug(`⚙️ Server listening on port: ${PORT}`);
    });

    await connectKafkaProducer();

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
