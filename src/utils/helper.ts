import { context, trace } from "@opentelemetry/api";

export function getTraceContext() {
  const spanContext = trace.getSpanContext(context.active());

  return {
    traceId: spanContext?.traceId,
    spanId: spanContext?.spanId,
  };
}