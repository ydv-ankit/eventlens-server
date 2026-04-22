import { Request, Response, NextFunction } from "express";

declare global {
    namespace Express {
        interface Response {
            success(statusCode: number, message: string, data?: Record<string, any>): void;
            error(statusCode: number, message: string): void;
        }
    }
}

export const apiResponseMiddleware = (_req: Request, res: Response, next: NextFunction) => {
    res.success = (statusCode, message, data) => {
        res.status(statusCode).json({ status: true, message, data });
    };
    res.error = (statusCode, message) => {
        res.status(statusCode).json({ status: false, message });
    };
    next();
};
