import { app } from "@/app";
import logger from "@/utils/logger";

const PORT = Number(process.env.PORT || 8080);

app.listen(PORT, () => {
    logger.info(`⚙️ Server listening on port: ${PORT}`);
});
  