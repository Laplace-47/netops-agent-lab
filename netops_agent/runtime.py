"""统一工具查找、参数校验、异步执行及安全的错误返回。"""

import json
import logging

from pydantic import ValidationError

from netops_agent.models import ToolCall, ToolResult
from netops_agent.registry import ToolRegistry

logger = logging.getLogger(__name__)


class ToolRuntime:
    def __init__(self, registry: ToolRegistry):
        self.registry = registry

    @staticmethod
    def _error(call: ToolCall, code: str, message: str, issues=None) -> ToolResult:
        metadata = {
            "tool_name": call.name, "tool_call_id": call.id,
            "error_code": code, "message": message,
        }
        if issues is not None:
            metadata["validation_errors"] = issues
        return ToolResult(type="error", metadata=metadata, content=[], indexes=[])

    async def execute(self, call: ToolCall) -> ToolResult:
        try:
            tool = self.registry.get(call.name)
        except KeyError:
            return self._error(call, "UNKNOWN_TOOL", "请求的工具未注册。")

        try:
            args = tool.args_model.model_validate(call.arguments)
        except ValidationError as exc:
            # 不把原始输入和异常上下文带入 API 返回。
            issues = json.loads(exc.json(
                include_input=False, include_context=False, include_url=False,
            ))
            return self._error(call, "INVALID_ARGUMENTS", "工具参数校验失败。", issues)

        try:
            raw_result = await tool.handler(args)
        except Exception:
            logger.exception("工具 %s 执行失败，call_id=%s", call.name, call.id)
            return self._error(call, "TOOL_EXECUTION_ERROR", "工具执行失败。")

        try:
            # 即使 handler 返回模型实例，也重新校验，避免实例被修改后绕过契约。
            # ToolResult 配置了 revalidate_instances="always"。
            result = ToolResult.model_validate(raw_result)
        except ValidationError:
            logger.exception("工具 %s 返回结构不合法", call.name)
            return self._error(call, "INVALID_TOOL_RESULT", "工具返回结果格式不合法。")

        result.metadata.update({"tool_name": call.name, "tool_call_id": call.id})
        return result
