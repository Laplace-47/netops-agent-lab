"""FastAPI 入口：python -m uvicorn main:app --host 127.0.0.1 --port 8000。"""

import os
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response

from netops_agent.models import CreateRunRequest, RunRecord
from netops_agent.planner import MockPlanner, Planner
from netops_agent.registry import ToolDefinition, ToolRegistry
from netops_agent.runtime import ToolRuntime
from netops_agent.service import RunService
from netops_agent.tools import FindWarningArgs, GetTrafficArgs, find_warning, get_traffic


def create_app(planner: Planner | None = None) -> FastAPI:
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="find_warning",
        description="按节点、等级和时间范围查询模拟告警",
        args_model=FindWarningArgs,
        handler=find_warning,
    ))
    registry.register(ToolDefinition(
        name="get_traffic",
        description="按节点和时间范围查询模拟流量利用率",
        args_model=GetTrafficArgs,
        handler=get_traffic,
    ))
    planner = planner or MockPlanner(timezone_name=os.getenv("NETOPS_TIMEZONE", "Asia/Shanghai"))
    service = RunService(planner, ToolRuntime(registry))
    app = FastAPI(title="NetOps Agent Lab", version="0.1.0")
    app.state.run_service = service

    @app.post("/runs", response_model=RunRecord, status_code=201)
    async def create_run(request: CreateRunRequest, response: Response) -> RunRecord:
        run = await service.create_run(request.message)
        response.headers["Location"] = f"/runs/{run.run_id}"
        return run

    @app.get("/runs/{run_id}", response_model=RunRecord)
    async def get_run(run_id: UUID) -> RunRecord:
        try:
            return service.get_run(run_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="Run 不存在。") from None

    return app


app = create_app()
