import { createHash, randomUUID } from "crypto";

export const generateApiKey = () => `el_${randomUUID().replace(/-/g, "")}`;
export const hashApiKey = (key: string) => createHash("sha256").update(key).digest("hex");
