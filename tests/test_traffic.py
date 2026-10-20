import asyncio

import pytest
from pydantic import ValidationError

from main import create_app
from netops_agent.models import ToolCall, ToolResult
from netops_agent.tools import GetTrafficArgs, TrafficRecord


@pytest.fixture
def traffic_arguments(valid_arguments):
    return {key: value for key, value in valid_arguments.items() if key != "severe_level"}


def test_traffic_is_registered_with_parameter_schema():
    registry = create_app().state.run_service.runtime.registry
    definition = registry.get("get_traffic")
    assert definition.args_model is GetTrafficArgs
    schema = definition.args_model.model_json_schema()
    assert set(schema["required"]) == {"node_id", "begin_time", "end_time"}
    assert schema["additionalProperties"] is False


def test_traffic_result_is_repeatable_and_marked_mock(traffic_arguments):
    runtime = create_app().state.run_service.runtime
    first = asyncio.run(runtime.execute(ToolCall(name="get_traffic", arguments=traffic_arguments)))
    second = asyncio.run(runtime.execute(ToolCall(name="get_traffic", arguments=traffic_arguments)))
    assert isinstance(first, ToolResult)
    assert first.type == second.type == "success"
    assert first.metadata["source"] == second.metadata["source"] == "mock"
    assert first.metadata["count"] == 1
    assert first.content == second.content
    assert first.indexes == second.indexes == ["mock-traffic-001"]
    record = TrafficRecord.model_validate(first.content[0])
    assert record.node_id == traffic_arguments["node_id"]
    assert record.utilization_percent == 72.5
    assert record.traffic_level == "HIGH"
    assert "模拟" in record.description
    assert record.begin_time == GetTrafficArgs.model_validate(traffic_arguments).begin_time
    assert record.end_time == GetTrafficArgs.model_validate(traffic_arguments).end_time


@pytest.mark.parametrize("patch", [
    {"node_id": ""}, {"node_id": 20}, {"begin_time": "invalid"},
    {"begin_time": "2026-10-11T00:00:00"},
    {"end_time": "2026-10-11T00:00:00+08:00"},
    {"end_time": "2026-10-10T23:59:59+08:00"}, {"unexpected": True},
])
def test_traffic_invalid_arguments_use_existing_runtime(traffic_arguments, patch):
    arguments = {**traffic_arguments, **patch}
    with pytest.raises(ValidationError):
        GetTrafficArgs.model_validate(arguments)
    result = asyncio.run(create_app().state.run_service.runtime.execute(
        ToolCall(name="get_traffic", arguments=arguments),
    ))
    assert result.type == "error"
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"


@pytest.mark.parametrize("missing", ["node_id", "begin_time", "end_time"])
def test_traffic_arguments_are_required(traffic_arguments, missing):
    arguments = {key: value for key, value in traffic_arguments.items() if key != missing}
    with pytest.raises(ValidationError):
        GetTrafficArgs.model_validate(arguments)
