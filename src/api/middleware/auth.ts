import { createClerkClient, getAuth } from "@clerk/express";
import { NextFunction, Request, Response } from "express";
import db from "@/shared/lib/config/db";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import { HTTP_CODE } from "@/shared/utils/constants";
import logger from "@/shared/utils/logger";

export interface DbUser {
  id: number;
  clerk_user_id: string;
  email: string;
  name: string;
  created_at: string;
}

declare global {
  namespace Express {
    interface Request {
      dbUser: DbUser;
    }
  }
}

const clerkClient = createClerkClient({
  secretKey: process.env.CLERK_SECRET_KEY,
});

const provisionUser = async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { userId: clerkUserId } = getAuth(req);
    if (!clerkUserId) {
      res.status(HTTP_CODE.UNAUTHORIZED).json({ status: false, message: "unauthorized" });
      return;
    }

    const existing = await db.query(SQL_QUERIES.GET_USER_BY_CLERK_ID, [clerkUserId]);
    if (existing.rowCount && existing.rowCount > 0) {
      req.dbUser = existing.rows[0];
      return next();
    }

    // First time: fetch details from Clerk and provision
    const clerkUser = await clerkClient.users.getUser(clerkUserId);
    const email = clerkUser.emailAddresses[0]?.emailAddress ?? "";
    const name = [clerkUser.firstName, clerkUser.lastName].filter(Boolean).join(" ");

    const result = await db.query(SQL_QUERIES.UPSERT_USER, [clerkUserId, email, name]);
    req.dbUser = result.rows[0];
    logger.debug(`provisioned new user clerk_user_id=${clerkUserId}`);
    next();
  } catch (error) {
    logger.error("auth middleware error: " + String(error));
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "authentication error");
  }
};

// clerkMiddleware() (registered globally in app.ts) populates req.auth from the JWT.
// provisionUser then checks it and JIT-provisions the DB user record.
export const authenticate = [provisionUser];
