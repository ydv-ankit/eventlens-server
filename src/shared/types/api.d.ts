export type EventRequestData = {
  event_name: string;
  user_id: string;
  session_id?: string;
  metadata: Record<any, any>;
  timestamp: Date;
};
