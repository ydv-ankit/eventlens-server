import { getSystemHealth } from "@/api/controllers/system.controller";
import { authenticate } from "@/api/middleware/auth";
import { Router } from "express";

const router = Router();

router.use(authenticate);
router.get("/health", getSystemHealth);

export default router;
