"""统一工具校验、单次执行超时、有限重试及安全的错误返回。"""

import asyncio
import json
import logging
import math

from pydantic import ValidationError

from netops_agent.models import ToolCall, ToolResult
from netops_agent.registry import ToolRegistry

logger = logging.getLogger(__name__)


class TemporaryToolError(Exception):
    """工具显式声明的临时故障，允许在预算内重试。"""


class BusinessToolError(Exception):
    """工具显式声明的业务失败，直接返回，不重试。"""


class ToolRuntime:
    """timeout 是单次异步执行的超时秒数，handler 应配合取消并释放资源。

    max_retries 为首次失败后额外重试次数，默认最多执行 1 + 3 = 4 次。
    参数/返回值校验不参与重试；外部取消继续向上传播。
    """

    def __init__(
        self, registry: ToolRegistry, *, timeout: float = 5.0, max_retries: int = 3,
    ):
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout 必须是大于 0 的有限秒数")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries 必须是非负整数")
        self.registry = registry
        self.timeout = timeout
        self.max_retries = max_retries

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

        for attempt in range(self.max_retries + 1):
            try:
                # 每次创建新的协程；超时后等待该次执行完成取消清理，再重试。
                raw_result = await asyncio.wait_for(tool.handler(args), timeout=self.timeout)
            except (TimeoutError, TemporaryToolError) as exc:
                code, message = (
                    ("TIMEOUT", "工具执行超时。") if isinstance(exc, TimeoutError)
                    else ("TEMPORARY_ERROR", "工具发生临时故障。")
                )
                logger.warning(
                    "工具 %s 执行失败，call_id=%s，code=%s，attempt=%s/%s",
                    call.name, call.id, code, attempt + 1, self.max_retries + 1,
                )
                if attempt == self.max_retries:
                    return self._error(call, code, message)
            except BusinessToolError:
                logger.warning("工具 %s 业务失败，call_id=%s", call.name, call.id)
                return self._error(call, "BUSINESS_ERROR", "工具业务处理失败。")
            except Exception:
                logger.exception("工具 %s 执行失败，call_id=%s", call.name, call.id)
                return self._error(call, "TOOL_EXECUTION_ERROR", "工具执行失败。")
            else:
                break

        try:
            # 即使 handler 返回模型实例，也重新校验，避免实例被修改后绕过契约。
            # ToolResult 配置了 revalidate_instances="always"。
            result = ToolResult.model_validate(raw_result)
        except ValidationError:
            logger.exception("工具 %s 返回结构不合法", call.name)
            return self._error(call, "INVALID_TOOL_RESULT", "工具返回结果格式不合法。")

        result.metadata.update({"tool_name": call.name, "tool_call_id": call.id})
        return result
