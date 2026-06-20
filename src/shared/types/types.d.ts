export type RetryQueue = {
  values: [EventRequestData & { project_id: number }];
  retry_count: number;
};

export type QueueEventEnvelope = {
  event: EventRequestData & { apiKey: string };
  trace: { traceparent?: string; tracestate?: string };
  queueAt: Number;
};

export type RedisEvent = QueueEventEnvelope;
export type KafkaEvent = QueueEventEnvelope;
export type RetryKafkaEvent = QueueEventEnvelope & {
  retryCount: number;
};
