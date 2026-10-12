import asyncio

import pytest

from netops_agent.models import ToolCall, ToolResult
from netops_agent.registry import ToolDefinition, ToolRegistry
from netops_agent.runtime import ToolRuntime
from netops_agent.tools import FindWarningArgs


def test_query_filters_node_severity_and_half_open_time_range(runtime, valid_arguments):
    result = asyncio.run(runtime.execute(ToolCall(name="find_warning", arguments=valid_arguments)))
    assert result.type == "success"
    assert result.indexes == ["mock-warning-001"]
    assert all(row["severe_level"] == "CRITICAL" for row in result.content)
    # 起始边界有记录；结束边界上的记录必须排除。
    narrow = {**valid_arguments, "begin_time": "2026-10-11T10:00:00+08:00",
              "end_time": "2026-10-11T10:00:01+08:00"}
    result = asyncio.run(runtime.execute(ToolCall(name="find_warning", arguments=narrow)))
    assert result.indexes == ["mock-warning-001"]


def test_invalid_enum_is_rejected_before_execution(valid_arguments):
    called = False

    async def handler(args):
        nonlocal called
        called = True
        raise AssertionError("非法参数不得进入工具")

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "测试校验", FindWarningArgs, handler))
    call = ToolCall(name="find_warning", arguments={**valid_arguments, "severe_level": "URGENT"})
    result = asyncio.run(ToolRuntime(registry).execute(call))
    assert result.type == "error"
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"
    assert result.content == [] and result.indexes == []
    assert called is False


@pytest.mark.parametrize("end_time", [
    "2026-10-11T00:00:00+08:00",
    "2026-10-10T23:59:59+08:00",
])
def test_invalid_time_range_is_rejected(runtime, valid_arguments, end_time):
    call = ToolCall(name="find_warning", arguments={**valid_arguments, "end_time": end_time})
    result = asyncio.run(runtime.execute(call))
    assert result.type == "error"
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"


@pytest.mark.parametrize("field", ["node_id", "severe_level", "begin_time", "end_time"])
def test_required_arguments_cannot_be_omitted(runtime, valid_arguments, field):
    arguments = {key: value for key, value in valid_arguments.items() if key != field}
    result = asyncio.run(runtime.execute(ToolCall(name="find_warning", arguments=arguments)))
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"


def test_naive_datetime_is_rejected(runtime, valid_arguments):
    arguments = {**valid_arguments, "begin_time": "2026-10-11T00:00:00"}
    result = asyncio.run(runtime.execute(ToolCall(name="find_warning", arguments=arguments)))
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"


def test_unknown_tool_is_rejected(runtime):
    result = asyncio.run(runtime.execute(ToolCall(name="unknown_tool", arguments={})))
    assert result.type == "error"
    assert result.metadata["error_code"] == "UNKNOWN_TOOL"


def test_tool_exception_becomes_safe_error_result(valid_arguments):
    async def broken(args):
        raise RuntimeError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "模拟失败", FindWarningArgs, broken))
    result = asyncio.run(ToolRuntime(registry).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.type == "error"
    assert result.metadata["error_code"] == "TOOL_EXECUTION_ERROR"
    assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in result.model_dump_json()


def test_invalid_tool_output_becomes_error_result(valid_arguments):
    async def broken(args):
        return {"type": "unexpected"}

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "模拟格式错误", FindWarningArgs, broken))
    result = asyncio.run(ToolRuntime(registry).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.metadata["error_code"] == "INVALID_TOOL_RESULT"


def test_registry_rejects_duplicate_name_and_exports_schema(registry):
    definition = registry.get("find_warning")
    with pytest.raises(ValueError, match="已注册"):
        registry.register(definition)
    schema = registry.schemas()[0]["function"]["parameters"]
    assert set(schema["required"]) == {"node_id", "severe_level", "begin_time", "end_time"}
    assert schema["$defs"]["SevereLevel"]["enum"] == ["INFO", "WARNING", "CRITICAL"]


def test_mutated_result_with_non_json_value_is_rejected(valid_arguments):
    async def broken(args):
        result = ToolResult(type="success", metadata={}, content=[], indexes=[])
        result.metadata["invalid_value"] = object()
        return result

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "模拟结果被修改", FindWarningArgs, broken))
    result = asyncio.run(ToolRuntime(registry).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.type == "error"
    assert result.metadata["error_code"] == "INVALID_TOOL_RESULT"
