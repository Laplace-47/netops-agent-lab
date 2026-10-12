"""注册可信工具定义，并为 Planner 导出参数 JSON Schema。"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from netops_agent.models import ToolResult

ArgsT = TypeVar("ArgsT", bound=BaseModel)


@dataclass(frozen=True)
class ToolDefinition(Generic[ArgsT]):
    name: str
    description: str
    args_model: type[ArgsT]
    handler: Callable[[ArgsT], Awaitable[ToolResult]]


class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, ToolDefinition[Any]] = {}

    def register(self, definition: ToolDefinition) -> None:
        if not definition.name.strip():
            raise ValueError("工具名称不能为空")
        if definition.name in self._tools:
            raise ValueError(f"工具 {definition.name} 已注册")
        self._tools[definition.name] = definition

    def get(self, name: str) -> ToolDefinition:
        return self._tools[name]

    def schemas(self) -> list[dict]:
        return [{
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.args_model.model_json_schema(),
            },
        } for tool in self._tools.values()]
