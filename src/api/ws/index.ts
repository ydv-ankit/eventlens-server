import { getRedisClient } from "@/shared/lib/config/redis";
import { REDIS_CHANNEL_EVENTS, REDIS_KEYS } from "@/shared/utils/constants";
import logger from "@/shared/utils/logger";
import type { IncomingMessage, Server } from "http";
import { WebSocket, WebSocketServer } from "ws";

interface WSClient {
  ws: WebSocket;
  topics: Set<string>;
}

const clients = new Set<WSClient>();

function send(ws: WebSocket, msg: unknown) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function broadcastSystemMetrics(payload: unknown) {
  for (const client of clients) {
    if (client.topics.has("system")) {
      send(client.ws, { type: "system_metrics", payload });
    }
  }
}

function broadcastLiveEvent(payload: { project_id: number; [k: string]: unknown }) {
  for (const client of clients) {
    if (
      client.topics.has("events") ||
      client.topics.has(`events:project:${payload.project_id}`)
    ) {
      send(client.ws, { type: "live_event", payload });
    }
  }
}

export function createWebSocketServer(httpServer: Server) {
  const wss        = new WebSocketServer({ server: httpServer, path: "/ws" });
  const subscriber = getRedisClient("WSSubscriber"); // subscribe mode — no other commands
  const reader     = getRedisClient("WSReader");     // reads atomic counters every second

  Promise.all([subscriber.connect(), reader.connect()])
    .then(async () => {
      // Live events: worker publishes one message per event
      await subscriber.subscribe(REDIS_CHANNEL_EVENTS, (message) => {
        try {
          broadcastLiveEvent(JSON.parse(message));
        } catch (e) {
          logger.error("WS live event parse error: " + String(e));
        }
      });

      // System metrics: API server owns the 1s publish loop.
      // getDel reads the window counter and resets it atomically —
      // multiple workers all INCR the same key; we read the combined total.
      setInterval(async () => {
        try {
          const [windowStr, totalStr, failedStr, inflightStr] = await Promise.all([
            reader.getDel(REDIS_KEYS.EVENTS_WINDOW),  // read + reset (1s window)
            reader.get(REDIS_KEYS.EVENTS_TOTAL),       // cumulative, never reset
            reader.getDel(REDIS_KEYS.FAILED_WINDOW),   // read + reset
            reader.get(REDIS_KEYS.INFLIGHT),           // current in-flight messages
          ]);

          broadcastSystemMetrics({
            eventsPerSec:         parseInt(windowStr   ?? "0"),
            totalEventsProcessed: parseInt(totalStr    ?? "0"),
            failedInsertionCount: parseInt(failedStr   ?? "0"),
            mainQueueDepth:       parseInt(inflightStr ?? "0"),
          });
        } catch (e) {
          logger.error("WS system metrics read failed: " + String(e));
        }
      }, 1000);

      logger.debug("WS Redis subscriber and reader ready");
    })
    .catch((e) => logger.error("WS Redis connect failed: " + String(e)));

  wss.on("connection", (ws: WebSocket, _req: IncomingMessage) => {
    const client: WSClient = { ws, topics: new Set() };
    clients.add(client);

    send(ws, { type: "connected" });

    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(raw.toString()) as { type: string; topics?: string[] };
        if (msg.type === "subscribe" && Array.isArray(msg.topics)) {
          msg.topics.forEach((t) => client.topics.add(t));
        }
        if (msg.type === "unsubscribe" && Array.isArray(msg.topics)) {
          msg.topics.forEach((t) => client.topics.delete(t));
        }
      } catch { /* ignore malformed frames */ }
    });

    ws.on("close", () => clients.delete(client));
    ws.on("error", () => { clients.delete(client); ws.terminate(); });
  });

  logger.debug("WebSocket server attached at /ws");
  return wss;
}
