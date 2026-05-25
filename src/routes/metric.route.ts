import { getMetrics, getPromMetrics } from "@/controllers/metric.controller";
import { Router } from "express";

const router = Router();

router.get("/", getPromMetrics);
router.get("/internal", getMetrics);

export default router;
