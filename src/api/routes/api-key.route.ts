import { deleteApiKey } from "@/api/controllers/api-key.controller";
import { Router } from "express";

const router = Router();

router.delete("/:id", deleteApiKey);

export default router;
