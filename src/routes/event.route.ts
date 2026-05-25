import { newEvent } from "@/controllers/event.controller";
import { Router } from "express";

const router = Router();

router.post("/", newEvent);

export default router;
