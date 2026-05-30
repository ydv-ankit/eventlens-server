import { createProject } from "@/api/controllers/project.controller";
import { Router } from "express";

const router = Router();

router.post("/create", createProject);

export default router;
