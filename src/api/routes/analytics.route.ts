import {
  getOverview,
  getEventVolume,
  getTopEvents,
  getRecentEvents,
} from "@/api/controllers/analytics.controller";
import { authenticate } from "@/api/middleware/auth";
import { requireProjectOwner } from "@/api/middleware/requireProjectOwner";
import { Router } from "express";

const router = Router();

router.use(authenticate);
router.use(requireProjectOwner);

router.get("/overview", getOverview);
router.get("/events/volume", getEventVolume);
router.get("/events/top", getTopEvents);
router.get("/events/recent", getRecentEvents);

export default router;
