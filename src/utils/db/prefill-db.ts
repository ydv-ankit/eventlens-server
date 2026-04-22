import { faker } from "@faker-js/faker";
import db from "@/lib/config/db";
import logger from "../logger";

const USER_COUNT = 500;
const PROJECT_COUNT = 50;
const EVENT_COUNT = 10_000;
const BATCH_SIZE = 500;

const EVENT_NAMES = [
    "page_view",
    "button_click",
    "sign_up",
    "sign_in",
    "sign_out",
    "form_submit",
    "feature_used",
    "app_open",
    "purchase",
    "error",
];

const eventMetadata = (eventName: string): Record<string, unknown> => {
    switch (eventName) {
        case "page_view":
            return { page: faker.internet.url(), referrer: faker.internet.url() };
        case "button_click":
            return { button: faker.word.noun(), page: faker.internet.url() };
        case "sign_up":
            return { method: faker.helpers.arrayElement(["email", "google", "github"]), plan: faker.helpers.arrayElement(["free", "pro", "enterprise"]) };
        case "sign_in":
            return { method: faker.helpers.arrayElement(["email", "google", "github"]) };
        case "sign_out":
            return { session_duration_ms: faker.number.int({ min: 1000, max: 3_600_000 }) };
        case "form_submit":
            return { form: faker.word.noun(), status: faker.helpers.arrayElement(["success", "error"]) };
        case "feature_used":
            return { feature: faker.word.noun(), duration_ms: faker.number.int({ min: 100, max: 30_000 }) };
        case "app_open":
            return { platform: faker.helpers.arrayElement(["ios", "android", "web"]), version: faker.system.semver() };
        case "purchase":
            return { plan: faker.helpers.arrayElement(["pro", "enterprise"]), amount_cents: faker.number.int({ min: 999, max: 99900 }) };
        case "error":
            return { message: faker.hacker.phrase(), code: faker.number.int({ min: 400, max: 599 }) };
        default:
            return {};
    }
};

const insertInBatches = async <T>(
    rows: T[],
    buildQuery: (batch: T[]) => { text: string; values: unknown[] },
    label: string,
) => {
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
        const batch = rows.slice(i, i + BATCH_SIZE);
        const { text, values } = buildQuery(batch);
        await db.query(text, values);
        logger.info(`${label}: inserted ${Math.min(i + BATCH_SIZE, rows.length)}/${rows.length}`);
    }
};

export const prefillDatabase = async () => {
    try {
        // ── Users ────────────────────────────────────────────────────────────
        const userNames = Array.from({ length: USER_COUNT }, () => faker.person.fullName());

        await insertInBatches(
            userNames,
            (batch) => {
                const placeholders = batch.map((_, j) => `($${j + 1})`).join(", ");
                return { text: `INSERT INTO analytics.user (name) VALUES ${placeholders} ON CONFLICT DO NOTHING`, values: batch };
            },
            "users",
        );

        // ── Projects ─────────────────────────────────────────────────────────
        // fetch actual user ids from db so foreign keys are valid
        const { rows: userRows } = await db.query<{ id: number }>(`SELECT id FROM analytics.user ORDER BY id`);
        const userIds = userRows.map((r) => r.id);

        const projects = Array.from({ length: PROJECT_COUNT }, () => ({
            name: faker.company.name(),
            apiKey: `api_${faker.string.uuid()}`,
            userId: faker.helpers.arrayElement(userIds),
        }));

        await insertInBatches(
            projects,
            (batch) => {
                const placeholders = batch.map((_, j) => `($${j * 3 + 1}, $${j * 3 + 2}, $${j * 3 + 3})`).join(", ");
                const values = batch.flatMap((p) => [p.name, p.apiKey, p.userId]);
                return { text: `INSERT INTO analytics.project (name, api_key, user_id) VALUES ${placeholders} ON CONFLICT DO NOTHING`, values };
            },
            "projects",
        );

        // ── Raw events ───────────────────────────────────────────────────────
        const { rows: projectRows } = await db.query<{ id: number }>(`SELECT id FROM analytics.project ORDER BY id`);
        const projectIds = projectRows.map((r) => r.id);

        const events = Array.from({ length: EVENT_COUNT }, () => {
            const name = faker.helpers.arrayElement(EVENT_NAMES);
            return {
                name,
                metadata: JSON.stringify(eventMetadata(name)),
                projectId: faker.helpers.arrayElement(projectIds),
                userId: faker.string.uuid(),
                timestamp: faker.date.recent({ days: 90 }),
            };
        });

        await insertInBatches(
            events,
            (batch) => {
                const placeholders = batch.map((_, j) => `($${j * 5 + 1}, $${j * 5 + 2}, $${j * 5 + 3}, $${j * 5 + 4}, $${j * 5 + 5})`).join(", ");
                const values = batch.flatMap((e) => [e.name, e.metadata, e.projectId, e.userId, e.timestamp]);
                return { text: `INSERT INTO analytics.raw_event (event_name, metadata, project_id, user_id, timestamp) VALUES ${placeholders} ON CONFLICT DO NOTHING`, values };
            },
            "events",
        );

        logger.info("database prefill completed");
    } catch (error) {
        logger.error("failed prefilling database -> " + error);
    }
};
