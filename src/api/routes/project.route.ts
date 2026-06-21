import { createProject, getProjects, getProjectById, updateProject, deleteProject } from "@/api/controllers/project.controller";
import { createApiKey, getApiKeys, deleteApiKey } from "@/api/controllers/api-key.controller";
import { authenticate } from "@/api/middleware/auth";
import { requireProjectOwner } from "@/api/middleware/requireProjectOwner";
import { Router } from "express";

const router = Router();

router.use(authenticate);

router.post("/", createProject);
router.get("/", getProjects);
router.get("/:id", requireProjectOwner, getProjectById);
router.patch("/:id", requireProjectOwner, updateProject);
router.delete("/:id", requireProjectOwner, deleteProject);

router.post("/:id/api-keys", requireProjectOwner, createApiKey);
router.get("/:id/api-keys", requireProjectOwner, getApiKeys);
router.delete("/:id/api-keys/:keyId", requireProjectOwner, deleteApiKey);

export default router;
