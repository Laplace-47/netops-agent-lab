import asyncio

import pytest

from netops_agent import runtime as runtime_module
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
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        raise AssertionError("非法参数不得进入工具")

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "测试校验", FindWarningArgs, handler))
    call = ToolCall(name="find_warning", arguments={**valid_arguments, "severe_level": "URGENT"})
    result = asyncio.run(ToolRuntime(registry).execute(call))
    assert result.type == "error"
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"
    assert result.content == [] and result.indexes == []
    assert calls == 0


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


def test_unknown_tool_is_rejected(runtime_factory, success_result):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        return success_result

    runtime = runtime_factory(handler)
    result = asyncio.run(runtime.execute(ToolCall(name="unknown_tool", arguments={})))
    assert result.type == "error"
    assert result.metadata["error_code"] == "UNKNOWN_TOOL"
    assert calls == 0


@pytest.mark.parametrize("error_type", [RuntimeError, ConnectionError, ValueError])
def test_tool_exception_becomes_safe_error_result(valid_arguments, error_type):
    calls = 0

    async def broken(args):
        nonlocal calls
        calls += 1
        raise error_type("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "模拟失败", FindWarningArgs, broken))
    result = asyncio.run(ToolRuntime(registry).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.type == "error"
    assert result.metadata["error_code"] == "TOOL_EXECUTION_ERROR"
    assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in result.model_dump_json()
    assert calls == 1


def test_invalid_tool_output_becomes_error_result(valid_arguments):
    calls = 0

    async def broken(args):
        nonlocal calls
        calls += 1
        return {"type": "unexpected"}

    registry = ToolRegistry()
    registry.register(ToolDefinition("find_warning", "模拟格式错误", FindWarningArgs, broken))
    result = asyncio.run(ToolRuntime(registry).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.metadata["error_code"] == "INVALID_TOOL_RESULT"
    assert calls == 1


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


@pytest.fixture
def success_result():
    return ToolResult(type="success", metadata={"source": "mock"}, content=[], indexes=[])


@pytest.fixture
def runtime_factory():
    def build(handler, **config):
        registry = ToolRegistry()
        registry.register(ToolDefinition("find_warning", "模拟执行策略", FindWarningArgs, handler))
        return ToolRuntime(registry, **config)

    return build


def test_first_attempt_succeeds_without_retry(runtime_factory, valid_arguments, success_result):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        return success_result

    runtime = runtime_factory(handler)
    assert runtime.timeout == 5.0
    assert runtime.max_retries == 3
    call = ToolCall(name="find_warning", arguments=valid_arguments)
    result = asyncio.run(runtime.execute(call))
    assert result.type == "success"
    assert result.content == []  # 空结果仍是成功，不会触发重试。
    assert result.metadata["tool_call_id"] == call.id
    assert calls == 1


@pytest.mark.parametrize("failures", [1, 3])
def test_timeout_then_retry_succeeds(runtime_factory, valid_arguments, success_result, failures):
    calls = 0
    cleanups = 0

    async def handler(args):
        nonlocal calls, cleanups
        calls += 1
        # 下一次尝试必须在上一次超时的协程完成清理后开始。
        assert cleanups == calls - 1
        if calls <= failures:
            try:
                await asyncio.Event().wait()
            finally:
                cleanups += 1
        return success_result

    runtime = runtime_factory(handler, timeout=0.01)
    result = asyncio.run(runtime.execute(ToolCall(name="find_warning", arguments=valid_arguments)))
    assert result.type == "success"
    assert calls == failures + 1
    assert cleanups == failures


@pytest.mark.parametrize("failures", [1, 3])
def test_temporary_error_then_retry_succeeds(
    runtime_factory, valid_arguments, success_result, failures,
):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        if calls <= failures:
            raise runtime_module.TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        return success_result

    result = asyncio.run(runtime_factory(handler).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.type == "success"
    assert calls == failures + 1  # 第四次成功必须仍在默认允许的预算内。


@pytest.mark.parametrize("failure, code", [
    ("timeout", "TIMEOUT"), ("temporary", "TEMPORARY_ERROR"),
])
@pytest.mark.parametrize("max_retries", [0, 1, 3])
def test_retry_exhaustion_counts_actual_attempts(
    runtime_factory, valid_arguments, failure, code, max_retries,
):
    calls = 0
    cleanups = 0

    async def handler(args):
        nonlocal calls, cleanups
        calls += 1
        if failure == "temporary":
            raise runtime_module.TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        try:
            await asyncio.Event().wait()
        finally:
            cleanups += 1

    runtime = runtime_factory(handler, timeout=0.01, max_retries=max_retries)
    call = ToolCall(name="find_warning", arguments=valid_arguments)
    result = asyncio.run(runtime.execute(call))
    assert result.type == "error"
    assert result.metadata["error_code"] == code
    assert result.metadata["tool_call_id"] == call.id
    assert result.content == [] and result.indexes == []
    assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in result.model_dump_json()
    assert calls == max_retries + 1  # max_retries=3 时，实际执行恰好 4 次。
    if failure == "timeout":
        assert cleanups == calls


@pytest.mark.parametrize("failures, expected_code", [
    (("timeout", "temporary") * 2, "TEMPORARY_ERROR"),
    (("temporary", "timeout") * 2, "TIMEOUT"),
])
def test_timeout_and_temporary_errors_share_retry_budget(
    runtime_factory, valid_arguments, failures, expected_code,
):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        if failures[calls - 1] == "timeout":
            await asyncio.Event().wait()
        raise runtime_module.TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    runtime = runtime_factory(handler, timeout=0.01)
    result = asyncio.run(runtime.execute(ToolCall(name="find_warning", arguments=valid_arguments)))
    assert result.metadata["error_code"] == expected_code  # 保留最后一次失败的分类。
    assert calls == 4


def test_business_error_is_not_retried(runtime_factory, valid_arguments):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        raise runtime_module.BusinessToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    result = asyncio.run(runtime_factory(handler).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.type == "error"
    assert result.metadata["error_code"] == "BUSINESS_ERROR"
    assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in result.model_dump_json()
    assert calls == 1


def test_returned_error_result_is_not_retried(runtime_factory, valid_arguments):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        return ToolResult(type="error", metadata={"error_code": "BUSINESS_ERROR"},
                          content=[], indexes=[])

    result = asyncio.run(runtime_factory(handler).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.metadata["error_code"] == "BUSINESS_ERROR"
    assert calls == 1


@pytest.mark.parametrize("error_type, code", [
    (runtime_module.BusinessToolError, "BUSINESS_ERROR"),
    (RuntimeError, "TOOL_EXECUTION_ERROR"),
])
def test_non_retryable_error_stops_existing_retry_sequence(
    runtime_factory, valid_arguments, error_type, code,
):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise runtime_module.TemporaryToolError("DO_NOT_EXPOSE_INTERNAL_DETAILS")
        raise error_type("DO_NOT_EXPOSE_INTERNAL_DETAILS")

    result = asyncio.run(runtime_factory(handler).execute(
        ToolCall(name="find_warning", arguments=valid_arguments),
    ))
    assert result.metadata["error_code"] == code
    assert "DO_NOT_EXPOSE_INTERNAL_DETAILS" not in result.model_dump_json()
    assert calls == 2


def test_external_cancellation_is_not_retried(runtime_factory, valid_arguments):
    calls = 0
    cleanups = 0

    async def scenario():
        started = asyncio.Event()

        async def handler(args):
            nonlocal calls, cleanups
            calls += 1
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanups += 1

        runtime = runtime_factory(handler)
        task = asyncio.create_task(runtime.execute(ToolCall(name="find_warning", arguments=valid_arguments)))
        try:
            await asyncio.wait_for(started.wait(), timeout=1)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    asyncio.run(scenario())
    assert calls == 1
    assert cleanups == 1


@pytest.mark.parametrize("config", [
    {"timeout": 0}, {"timeout": -1}, {"timeout": float("nan")},
    {"timeout": float("inf")}, {"timeout": True},
    {"max_retries": -1}, {"max_retries": 1.5}, {"max_retries": True},
])
def test_invalid_execution_config_is_rejected(runtime_factory, success_result, config):
    calls = 0

    async def handler(args):
        nonlocal calls
        calls += 1
        return success_result

    with pytest.raises(ValueError):
        runtime_factory(handler, **config)
    assert calls == 0
