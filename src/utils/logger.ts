import { createLogger, format, transports } from "winston";
const { combine, timestamp, json, colorize } = format;
import { getTraceContext } from "./helper";

const traceFormat = format((info) => {
  const spanContext = getTraceContext();

  if (spanContext) {
    info.traceId = spanContext.traceId;
    info.spanId = spanContext.spanId;
  }

  return info;
});

// Custom format for console logging with colors
const consoleLogFormat = format.combine(
  format.colorize(),
  format.printf(({ level, message, timestamp, traceId, spanId }) => {
    return `${level} (${timestamp}): ${message} - traceId=${traceId ?? "-"} spanId=${spanId ?? "-"}`;
  }),
);

// Create a Winston logger
const logger = createLogger({
  level: "info",
  format: combine(traceFormat(), colorize(), timestamp(), json()),
  transports: [
    new transports.Console({
      format: consoleLogFormat,
    }),
    // new transports.File({ filename: "app.log" }),  // uncomment to write logs to a file
  ],
});

export default logger;
