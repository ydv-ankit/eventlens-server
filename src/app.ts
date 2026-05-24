import express from "express";
import projectRoutes from "@/routes/project.route"
import eventRoutes from "@/routes/event.route"
import metricRoutes from "@/routes/metric.route"
import logger from "@/utils/logger";
import morgan from "morgan";
import "@/utils/api-response";
import { HTTP_CODE, TOTAL_REQUESTS } from "./utils/constants";
import { redisClient } from "./lib/config/redis";
import { failedRequestsCounter, totalRequestsCounter } from "./utils/monitoring/prom";

const app = express();

// middlewares
app.use(express.json())

const morganFormat = ":method :url :status :response-time ms";
app.use(
  morgan(morganFormat, {
    stream: {
      write: (message) => {
        const logObject = {
          method: message.split(" ")[0],
          url: message.split(" ")[1],
          status: message.split(" ")[2],
          responseTime: message.split(" ")[3],
        };
        logger.info(JSON.stringify(logObject));
      },
    },
  })
);

app.use(
  async (
    req: express.Request, 
    res: express.Response, 
    next: express.NextFunction
  ) => {
    res.on("finish", () => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        failedRequestsCounter.inc();
      }
    });

    totalRequestsCounter.inc();
    try{
      await redisClient.incr(TOTAL_REQUESTS);
    } finally{
      next()
    }
  }
)

// routes
app.get("/", (_, res) => {
  res.success(HTTP_CODE.OK, "hello there")
});

app.use("/project", projectRoutes)

app.use("/event", eventRoutes);

app.use("/metrics", metricRoutes);

app.use("/*path", (req, res)=> {
  res.error(HTTP_CODE.NOT_FOUND, "route not found")
})

app.use((err: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error(err.message);
  res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "Internal server error");
});

export {app};
