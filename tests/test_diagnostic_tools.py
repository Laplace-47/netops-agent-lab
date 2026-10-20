import asyncio

import pytest
from pydantic import ValidationError

from main import create_app
from netops_agent.models import ToolCall, ToolResult
from netops_agent.tools import DiagnoseNodeArgs, GetDeviceArgs


@pytest.mark.parametrize("name, model, required", [
    ("get_device", GetDeviceArgs, {"node_id"}),
    ("diagnose_node", DiagnoseNodeArgs, {"node_id", "device_info", "warnings", "traffic"}),
])
def test_diagnostic_tools_use_existing_registry(name, model, required):
    definition = create_app().state.run_service.runtime.registry.get(name)
    assert definition.args_model is model
    assert set(model.model_json_schema()["required"]) == required


def test_device_result_is_repeatable_and_explicitly_mock():
    async def scenario():
        runtime = create_app().state.run_service.runtime
        call = ToolCall(name="get_device", arguments={"node_id": "node_20"})
        first = await runtime.execute(call)
        second = await runtime.execute(call)
        assert first == second
        assert first.type == "success" and first.metadata["source"] == "mock"
        assert first.content[0]["node_id"] == "node_20"
        assert first.content[0]["state"] == "UP"
        assert "模拟" in first.content[0]["description"]
        assert first.indexes == [first.content[0]["device_id"]]
    asyncio.run(scenario())


@pytest.mark.parametrize("node", ["", " ", 20, None])
def test_invalid_device_node_is_rejected(node):
    result = asyncio.run(create_app().state.run_service.runtime.execute(
        ToolCall(name="get_device", arguments={"node_id": node}),
    ))
    assert result.metadata["error_code"] == "INVALID_ARGUMENTS"


@pytest.fixture
def diagnosis_arguments():
    return {
        "node_id": "node_20",
        "device_info": [{"device_id": "custom-device", "node_id": "node_20",
                         "name": "模拟设备", "state": "UP", "description": "模拟来源"}],
        "warnings": [], "traffic": [],
    }


@pytest.mark.parametrize("missing", ["node_id", "device_info", "warnings", "traffic"])
def test_diagnosis_requires_all_inputs(diagnosis_arguments, missing):
    del diagnosis_arguments[missing]
    with pytest.raises(ValidationError):
        DiagnoseNodeArgs.model_validate(diagnosis_arguments)


@pytest.mark.parametrize("case", ["empty_device", "wrong_node", "malformed_warning", "bad_utilization"])
def test_invalid_diagnostic_evidence_is_rejected(diagnosis_arguments, valid_arguments, case):
    if case == "empty_device":
        diagnosis_arguments["device_info"] = []
    elif case == "wrong_node":
        diagnosis_arguments["device_info"][0]["node_id"] = "node_21"
    elif case == "malformed_warning":
        diagnosis_arguments["warnings"] = [{"node_id": "node_20"}]
    else:
        diagnosis_arguments["traffic"] = [{
            "traffic_id": "custom-traffic", "node_id": "node_20",
            "begin_time": valid_arguments["begin_time"], "end_time": valid_arguments["end_time"],
            "utilization_percent": 101, "traffic_level": "HIGH", "description": "模拟",
        }]
    result = asyncio.run(create_app().state.run_service.runtime.execute(
        ToolCall(name="diagnose_node", arguments=diagnosis_arguments),
    ))
    assert result.type == "error" and result.metadata["error_code"] == "INVALID_ARGUMENTS"


def test_empty_successful_evidence_does_not_invent_measurements(diagnosis_arguments):
    result = asyncio.run(create_app().state.run_service.runtime.execute(
        ToolCall(name="diagnose_node", arguments=diagnosis_arguments),
    ))
    assert isinstance(result, ToolResult) and result.type == "success"
    assert result.metadata["source"] == "mock"
    row = result.content[0]
    assert row["warning_count"] == row["critical_warning_count"] == 0
    assert row["max_utilization_percent"] is None
    assert row["traffic_levels"] == []
    assert row["evidence"]["device_ids"] == ["custom-device"]
    assert row["evidence"]["warning_ids"] == row["evidence"]["traffic_ids"] == []
