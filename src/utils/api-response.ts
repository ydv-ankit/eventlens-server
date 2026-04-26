import express from "express";

declare global {
    namespace Express {
        interface Response {
            success(statusCode: number, message: string, data?: Record<string, any>): void;
            error(statusCode: number, message: string): void;
        }
    }
}

express.response.success = function (statusCode, message, data) {
    this.status(statusCode).json({ status: true, message, data });
};
express.response.error = function (statusCode, message) {
    this.status(statusCode).json({ status: false, message });
};