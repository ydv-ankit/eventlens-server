export const SQL_QUERIES = {
  // Users
  UPSERT_USER: `
    INSERT INTO analytics.user (clerk_user_id, email, name)
    VALUES ($1, $2, $3)
    ON CONFLICT (clerk_user_id) DO UPDATE
      SET email = EXCLUDED.email, name = EXCLUDED.name
    RETURNING id, clerk_user_id, email, name, created_at
  `,
  GET_USER_BY_CLERK_ID: `
    SELECT id, clerk_user_id, email, name, created_at
    FROM analytics.user
    WHERE clerk_user_id = $1
  `,

  // Projects
  CREATE_PROJECT: `
    INSERT INTO analytics.project (name, description, clerk_user_id)
    VALUES ($1, $2, $3)
    RETURNING id, name, description, created_at
  `,
  GET_PROJECTS: `
    SELECT id, name, description, created_at
    FROM analytics.project
    WHERE clerk_user_id = $1
    ORDER BY created_at DESC
  `,
  GET_PROJECT_BY_ID: `
    SELECT id, name, description, created_at
    FROM analytics.project
    WHERE id = $1 AND clerk_user_id = $2
  `,
  UPDATE_PROJECT: `
    UPDATE analytics.project
    SET name = COALESCE($1, name), description = COALESCE($2, description)
    WHERE id = $3 AND clerk_user_id = $4
    RETURNING id, name, description, created_at
  `,
  DELETE_PROJECT: `DELETE FROM analytics.project WHERE id = $1 AND clerk_user_id = $2 RETURNING id`,

  // API Keys
  CREATE_API_KEY: `
    INSERT INTO analytics.api_key (project_id, key_hash)
    VALUES ($1, $2)
    RETURNING id, project_id, is_active, created_at
  `,
  GET_API_KEYS_BY_PROJECT: `
    SELECT id, project_id, last_used_at, is_active, created_at
    FROM analytics.api_key
    WHERE project_id = $1
    ORDER BY created_at DESC
  `,
  DELETE_API_KEY: `DELETE FROM analytics.api_key WHERE id = $1 RETURNING id`,
  // Also updates last_used_at on every successful lookup
  GET_PROJECT_ID_BY_API_KEY: `
    UPDATE analytics.api_key
    SET last_used_at = NOW()
    WHERE key_hash = $1 AND is_active = true
    RETURNING project_id
  `,

  // Events
  BATCH_EVENT_INSERT: `INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp, session_id) VALUES `,
  GET_EVENTS: `
    SELECT id, event_name, metadata, project_id, user_id, timestamp, created_at
    FROM analytics.raw_event
    WHERE project_id = $1
      AND ($2::text IS NULL OR event_name ILIKE '%' || $2 || '%')
      AND ($3::text IS NULL OR user_id ILIKE '%' || $3 || '%')
      AND ($4::timestamptz IS NULL OR COALESCE(timestamp, created_at) >= $4)
      AND ($5::timestamptz IS NULL OR COALESCE(timestamp, created_at) <= $5)
      AND ($6::int IS NULL OR id > $6)
    ORDER BY id ASC
    LIMIT $7
  `,

  // Analytics
  GET_ANALYTICS_OVERVIEW: `
    SELECT
      (SELECT COUNT(*) FROM analytics.raw_event
        WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= CURRENT_DATE)::int             AS today_events,
      (SELECT COUNT(DISTINCT user_id) FROM analytics.raw_event
        WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= CURRENT_DATE)::int             AS active_users,
      (SELECT ROUND(COUNT(*)::numeric / 60.0, 2) FROM analytics.raw_event
        WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= NOW() - INTERVAL '60 seconds') AS events_per_sec,
      (SELECT COUNT(*)::int FROM analytics.project)                           AS project_count
  `,

  GET_EVENT_VOLUME_1H: `
    SELECT
      date_trunc('hour', COALESCE(timestamp, created_at))
        + (EXTRACT(MINUTE FROM COALESCE(timestamp, created_at))::int / 5) * INTERVAL '5 minutes' AS bucket,
      COUNT(*)::int AS count
    FROM analytics.raw_event
    WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= NOW() - INTERVAL '1 hour'
    GROUP BY bucket ORDER BY bucket ASC
  `,
  GET_EVENT_VOLUME_24H: `
    SELECT date_trunc('hour', COALESCE(timestamp, created_at)) AS bucket, COUNT(*)::int AS count
    FROM analytics.raw_event
    WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= NOW() - INTERVAL '24 hours'
    GROUP BY bucket ORDER BY bucket ASC
  `,
  GET_EVENT_VOLUME_7D: `
    SELECT
      date_trunc('day', COALESCE(timestamp, created_at))
        + (EXTRACT(HOUR FROM COALESCE(timestamp, created_at))::int / 6) * INTERVAL '6 hours' AS bucket,
      COUNT(*)::int AS count
    FROM analytics.raw_event
    WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= NOW() - INTERVAL '7 days'
    GROUP BY bucket ORDER BY bucket ASC
  `,
  GET_EVENT_VOLUME_30D: `
    SELECT date_trunc('day', COALESCE(timestamp, created_at)) AS bucket, COUNT(*)::int AS count
    FROM analytics.raw_event
    WHERE project_id = $1 AND COALESCE(timestamp, created_at) >= NOW() - INTERVAL '30 days'
    GROUP BY bucket ORDER BY bucket ASC
  `,

  GET_TOP_EVENTS: `
    SELECT event_name, COUNT(*)::int AS count
    FROM analytics.raw_event
    WHERE project_id = $1
    GROUP BY event_name
    ORDER BY count DESC
    LIMIT $2
  `,

  GET_RECENT_EVENTS: `
    SELECT id, event_name, user_id, metadata, COALESCE(timestamp, created_at) AS timestamp
    FROM analytics.raw_event
    WHERE project_id = $1
    ORDER BY COALESCE(timestamp, created_at) DESC
    LIMIT $2
  `,

  // System
  DB_PING: `SELECT 1`,

  // Users
  GET_USER_TIMELINE: `
    SELECT id, event_name, metadata, project_id, user_id, COALESCE(timestamp, created_at) AS timestamp, created_at
    FROM analytics.raw_event
    WHERE user_id = $1
    ORDER BY COALESCE(timestamp, created_at) ASC
    LIMIT 200
  `,
} as const;
