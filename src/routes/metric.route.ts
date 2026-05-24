import { getMetrics } from "@/controllers/metric.controller";
import {Router} from "express"

const router = Router()

router.get("/app", getMetrics)

export default router;