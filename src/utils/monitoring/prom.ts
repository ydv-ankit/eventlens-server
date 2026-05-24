import client from 'prom-client'

export const totalRequestsCounter = new client.Counter({
  name: 'total_requests_count',
  help: 'Total number of HTTP requests received by the application',
});

export const failedRequestsCounter = new client.Counter({
  name: 'failed_requests_count',
  help: 'Total number of HTTP requests completed with a non-2xx status code',
});

export const totalEventsProcessedCounter = new client.Counter({
    name: 'total_events_processed_count',
    help: 'Total number of events successfully processed and inserted by workers',
});
  
export const mainQueueDepthGauge = new client.Gauge({
    name: 'main_queue_depth',
    help: 'Current number of events waiting in the main ingestion queue',
});

export const failedInsertionsCounter = new client.Gauge({
    name: 'failed_insertions_count',
    help: 'Current number of failed event insertions recorded by the application',
});
  
export const batchInsertionsGauge = new client.Gauge({
    name: 'batch_insertions',
    help: 'Current number of events included in the most recent worker batch insert',
});
