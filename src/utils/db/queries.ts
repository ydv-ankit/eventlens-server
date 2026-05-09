export const SQL_QUERIES = {
    GET_PROJECT_ID_BY_API_KEY: `SELECT id FROM analytics.project WHERE api_key = $1`,
    BATCH_EVENT_INSERT: `INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp) VALUES `,
};
