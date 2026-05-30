import { getMetrics, getPromMetrics } from "@/api/controllers/metric.controller";
import { Router } from "express";

const router = Router();

router.get("/", getPromMetrics);
router.get("/internal", getMetrics);

export default router;
