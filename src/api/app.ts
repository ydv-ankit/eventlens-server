import express from "express";
import cors from "cors";
import { clerkMiddleware } from "@clerk/express";
import projectRoutes from "@/api/routes/project.route";
import eventRoutes from "@/api/routes/event.route";
import metricRoutes from "@/api/routes/metric.route";
import analyticsRoutes from "@/api/routes/analytics.route";
import systemRoutes from "@/api/routes/system.route";
import userRoutes from "@/api/routes/user.route";
import { ENV } from "@/shared/utils/env";
import logger from "@/shared/utils/logger";
import morgan from "morgan";
import "@/shared/utils/api-response";
import { HTTP_CODE } from "@/shared/utils/constants";
import {
  failedRequestsCounter,
  totalRequestsCounter,
} from "@/shared/utils/monitoring/prom";
import opentelemetry, { SpanStatusCode, trace } from "@opentelemetry/api";
import { isKafkaProducerConnected } from "@/shared/lib/config/kafka";

const app = express();
const tracer = opentelemetry.trace.getTracer("eventlens-http");
const UNTRACED_ROUTES = new Set(["/metrics", "/healthz", "/readyz"]);

// middlewares
app.use(cors({
  origin: ENV.CORS_ORIGINS,
  credentials: true,
  allowedHeaders: ["Content-Type", "Authorization"],
}));
app.use(express.json());
app.use(clerkMiddleware());

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
        logger.debug(JSON.stringify(logObject));
      },
    },
  }),
);

app.use(
  async (
    req: express.Request,
    res: express.Response,
    next: express.NextFunction,
  ) => {
    // exclude certain requests
    if (req.method === "GET" && UNTRACED_ROUTES.has(req.path)) {
      next();
      return;
    }

    const requestSpan = tracer.startSpan(`HTTP ${req.method} ${req.path}`, {
      attributes: {
        "http.method": req.method,
        "http.route": req.path,
        "http.target": req.originalUrl,
      },
    });
    let spanEnded = false;

    const endSpan = () => {
      if (spanEnded) return;
      spanEnded = true;

      requestSpan.setAttribute("http.status_code", res.statusCode);
      if (res.statusCode >= 400) {
        requestSpan.setStatus({
          code: SpanStatusCode.ERROR,
          message: `HTTP ${res.statusCode}`,
        });
      } else {
        requestSpan.setStatus({ code: SpanStatusCode.OK });
      }
      requestSpan.end();
    };

    res.on("finish", endSpan);
    res.on("close", endSpan);

    res.on("finish", () => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        failedRequestsCounter.inc();
      }
    });

    return opentelemetry.context.with(
      trace.setSpan(opentelemetry.context.active(), requestSpan),
      async () => {
        totalRequestsCounter.inc();
        next();
      },
    );
  },
);

// routes
app.get("/", (_, res) => {
  res.success(HTTP_CODE.OK, "hello there");
});

app.get("/healthz", (_req, res) => {
  res.status(HTTP_CODE.OK).json({
    status: true,
    message: "ok",
  });
});

app.get("/readyz", (_req, res) => {
  if (!isKafkaProducerConnected()) {
    res.status(HTTP_CODE.SERVICE_UNAVAILABLE).json({
      status: false,
      message: "kafka producer unavailable",
    });
    return;
  }

  res.status(HTTP_CODE.OK).json({
    status: true,
    message: "ready",
  });
});

app.use("/project", projectRoutes);
app.use("/event", eventRoutes);
app.use("/metrics", metricRoutes);
app.use("/analytics", analyticsRoutes);
app.use("/system", systemRoutes);
app.use("/users", userRoutes);

app.use("/*path", (req, res) => {
  res.error(HTTP_CODE.NOT_FOUND, "route not found");
});

app.use(
  (
    err: Error,
    _req: express.Request,
    res: express.Response,
    _next: express.NextFunction,
  ) => {
    logger.error(err.message);
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "Internal server error");
  },
);

export { app };
