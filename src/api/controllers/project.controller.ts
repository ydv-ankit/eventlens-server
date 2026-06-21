import db from "@/shared/lib/config/db";
import { HTTP_CODE } from "@/shared/utils/constants";
import { SQL_QUERIES } from "@/shared/utils/db/queries";
import { NextFunction, Request, Response } from "express";

const createProject = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { name, description } = req.body;
    if (!name?.trim()) {
      res.error(HTTP_CODE.BAD_REQUEST, "project name is required");
      return;
    }
    const result = await db.query(SQL_QUERIES.CREATE_PROJECT, [
      name.trim(),
      description ?? null,
      req.dbUser.clerk_user_id,
    ]);
    res.success(HTTP_CODE.CREATED, "project created", result.rows[0]);
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error creating project");
  }
};

const getProjects = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const result = await db.query(SQL_QUERIES.GET_PROJECTS, [req.dbUser.clerk_user_id]);
    res.success(HTTP_CODE.OK, "projects fetched", result.rows);
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching projects");
  }
};

const getProjectById = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { id } = req.params;
    const result = await db.query(SQL_QUERIES.GET_PROJECT_BY_ID, [id, req.dbUser.clerk_user_id]);
    if (result.rowCount === 0) {
      res.error(HTTP_CODE.NOT_FOUND, "project not found");
      return;
    }
    res.success(HTTP_CODE.OK, "project fetched", result.rows[0]);
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error fetching project");
  }
};

const updateProject = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { id } = req.params;
    const { name, description } = req.body;
    if (!name && description === undefined) {
      res.error(HTTP_CODE.BAD_REQUEST, "nothing to update");
      return;
    }
    const result = await db.query(SQL_QUERIES.UPDATE_PROJECT, [
      name ?? null,
      description ?? null,
      id,
      req.dbUser.clerk_user_id,
    ]);
    if (result.rowCount === 0) {
      res.error(HTTP_CODE.NOT_FOUND, "project not found");
      return;
    }
    res.success(HTTP_CODE.OK, "project updated", result.rows[0]);
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error updating project");
  }
};

const deleteProject = async (req: Request, res: Response, _next: NextFunction) => {
  try {
    const { id } = req.params;
    const result = await db.query(SQL_QUERIES.DELETE_PROJECT, [id, req.dbUser.clerk_user_id]);
    if (result.rowCount === 0) {
      res.error(HTTP_CODE.NOT_FOUND, "project not found");
      return;
    }
    res.success(HTTP_CODE.OK, "project deleted");
  } catch (error) {
    res.error(HTTP_CODE.INTERNAL_SERVER_ERROR, "error deleting project");
  }
};

export { createProject, getProjects, getProjectById, updateProject, deleteProject };
