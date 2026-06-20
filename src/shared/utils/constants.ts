export const HTTP_CODE = {
  // 2xx
  OK: 200,
  CREATED: 201,
  NO_CONTENT: 204,

  // 3xx
  MOVED_PERMANENTLY: 301,
  NOT_MODIFIED: 304,

  // 4xx
  BAD_REQUEST: 400,
  UNAUTHORIZED: 401,
  FORBIDDEN: 403,
  NOT_FOUND: 404,
  METHOD_NOT_ALLOWED: 405,
  CONFLICT: 409,
  UNPROCESSABLE_ENTITY: 422,
  TOO_MANY_REQUESTS: 429,

  // 5xx
  INTERNAL_SERVER_ERROR: 500,
  BAD_GATEWAY: 502,
  SERVICE_UNAVAILABLE: 503,
};

export const BATCH_INSERTION_LIMIT = 500;
export const EVENT_QUEUE_KEY = "event_queue";
export const EVENT_QUEUE_MAX_SIZE = 1_000_000_000;
export const RETRY_COUNT_LIMIT = 3;
export const RETRY_EVENT_QUEUE_KEY = "retry_event_queue";
export const RETRY_EVENT_QUEUE_LIMIT = 5_000_000;
export const RETRY_QUEUE_FAILED_RETRY_DELAY = 1_000;
export const REDIS_RETRY_DELAY_MS = 2_000;
export const KAFKA_TOPIC = "eventlens-events";
export const KAFKA_RETRY_TOPIC = "eventlens-events-retry";
export const KAFKA_CONSUMER_GROUP = "eventlens-worker-group";
export const KAFKA_RETRY_CONSUMER_GROUP = "eventlens-retry-worker-group";
export const KAFKA_TOPIC_PARTITIONS = 4;

// metrics
export const TOTAL_REQUESTS = "TOTAL_REQUESTS";
export const TOTAL_EVENTS_PROCESSED = "TOTAL_EVENTS_PROCESSED";
export const FAILED_INSERTION_COUNT = "FAILED_INSERTION_COUNT";
