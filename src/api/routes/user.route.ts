import { getUserTimeline } from "@/api/controllers/user.controller";
import { authenticate } from "@/api/middleware/auth";
import { Router } from "express";

const router = Router();
router.use(authenticate);
router.get("/:id/events", getUserTimeline);

export default router;
