"""FastAPI 入口：python -m uvicorn main:app --host 127.0.0.1 --port 8000。"""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response

from netops_agent.models import CreateRunRequest, RunRecord
from netops_agent.planner import MockPlanner, Planner
from netops_agent.registry import ToolDefinition, ToolRegistry
from netops_agent.runtime import ToolRuntime
from netops_agent.repository import SQLiteRunRepository
from netops_agent.service import RunService
from netops_agent.tools import (
    DiagnoseNodeArgs, FindWarningArgs, GetDeviceArgs, GetTrafficArgs,
    diagnose_node, find_warning, get_device, get_traffic,
)


def create_app(planner: Planner | None = None, *, db_path: str | Path = ":memory:") -> FastAPI:
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
    registry.register(ToolDefinition(
        name="get_device", description="按节点查询模拟设备信息",
        args_model=GetDeviceArgs, handler=get_device,
    ))
    registry.register(ToolDefinition(
        name="diagnose_node", description="根据传入的模拟设备、告警和流量证据生成诊断",
        args_model=DiagnoseNodeArgs, handler=diagnose_node,
    ))
    planner = planner or MockPlanner(timezone_name=os.getenv("NETOPS_TIMEZONE", "Asia/Shanghai"))
    repository = SQLiteRunRepository(db_path)
    service = RunService(planner, ToolRuntime(registry), repository)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            repository.initialize()
            await service.start()
            yield
        finally:
            try:
                await service.stop()
            finally:
                repository.close()

    app = FastAPI(title="NetOps Agent Lab", version="0.1.0", lifespan=lifespan)
    app.state.run_service = service

    @app.post("/runs", response_model=RunRecord, status_code=202)
    async def create_run(request: CreateRunRequest, response: Response) -> RunRecord:
        try:
            run = await service.create_run(request.message)
        except RuntimeError:
            raise HTTPException(status_code=503, detail="服务正在关闭，暂不接收任务。") from None
        response.headers["Location"] = f"/runs/{run.run_id}"
        return run

    @app.get("/runs/{run_id}", response_model=RunRecord)
    async def get_run(run_id: UUID) -> RunRecord:
        try:
            return service.get_run(run_id)
        except KeyError:
            raise HTTPException(status_code=404, detail="Run 不存在。") from None

    return app


app = create_app(db_path=os.getenv("NETOPS_DB_PATH", "data/netops_agent.db"))
