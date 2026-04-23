export type RetryQueue = {
  values: [EventRequestData & { project_id: number }];
  retry_count: number;
};
