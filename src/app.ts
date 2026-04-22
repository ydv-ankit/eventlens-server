import express from "express";
import projectRoutes from "@/routes/project.route"
import logger from "@/utils/logger";
import morgan from "morgan";
import { apiResponseMiddleware } from "@/utils/api-response";
import { HTTP_CODE } from "./utils/constants";

const app = express();


// middlewares
app.use(apiResponseMiddleware);

const morganFormat = ":method :url :status :response-time ms";
app.use(
  morgan(morganFormat, {
    stream: {
      write: (message) => {
        const logObject = {
          method: message.split(" ")[0],
          url: message.split(" ")[1],
          status: message.split(" ")[2],
          responseTime: message.split(" ")[3],
        };
        logger.info(JSON.stringify(logObject));
      },
    },
  })
);

// routes
app.get("/", (_, res) => {
  res.success(HTTP_CODE.OK, "hello there")
});

app.use("/project", projectRoutes)

app.use("/*path", (req, res)=> {
  res.error(HTTP_CODE.NOT_FOUND, "route not found")
})

export {app};