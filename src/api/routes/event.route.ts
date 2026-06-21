import { newEvent, getEvents } from "@/api/controllers/event.controller";
import { authenticate } from "@/api/middleware/auth";
import { requireProjectOwner } from "@/api/middleware/requireProjectOwner";
import { Router } from "express";

const router = Router();

router.post("/", newEvent);
router.get("/", authenticate, requireProjectOwner, getEvents);

export default router;
