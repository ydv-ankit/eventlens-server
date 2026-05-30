export type RetryQueue = {
  values: [EventRequestData & { project_id: number }];
  retry_count: number;
};

export type RedisEvent = {
  event: EventRequestData & { apiKey: string };
  trace: { traceparent?: string; tracestate?: string };
  queueAt: Number;
};
