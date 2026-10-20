import pytest
from pydantic import ValidationError

from netops_agent.models import DAGPlan, DAGStep, ResultBinding, ToolCall


def make_step(step_id, dependencies=(), bindings=(), arguments=None):
    return DAGStep(
        tool_call=ToolCall(id=step_id, name="probe", arguments=arguments or {}),
        depends_on=list(dependencies), bindings=list(bindings),
    )


def test_valid_dag_accepts_unordered_steps_and_reuses_call_ids():
    source = make_step("source")
    sink = make_step("sink", ["source"], [
        ResultBinding(source_step_id="source", target_argument="records"),
    ])
    plan = DAGPlan(steps=[sink, source])
    assert plan.mode == "dag"
    assert [step.tool_call.id for step in plan.steps] == ["sink", "source"]
    assert plan.steps[0].bindings[0].target_argument == "records"


@pytest.mark.parametrize("case", [
    "empty", "duplicate_id", "unknown_dependency", "self_dependency", "cycle",
    "non_direct_binding", "unknown_binding", "duplicate_binding", "literal_collision",
    "duplicate_dependency",
])
def test_invalid_dag_is_rejected(case):
    steps = [
        make_step("a").model_dump(mode="json"),
        make_step("b", ["a"]).model_dump(mode="json"),
        make_step("c", ["b"]).model_dump(mode="json"),
    ]
    if case == "empty":
        steps = []
    elif case == "duplicate_id":
        steps[1]["tool_call"]["id"] = "a"
    elif case == "unknown_dependency":
        steps[1]["depends_on"] = ["missing"]
    elif case == "self_dependency":
        steps[0]["depends_on"] = ["a"]
    elif case == "cycle":
        steps[0]["depends_on"] = ["c"]
    elif case == "duplicate_dependency":
        steps[1]["depends_on"] = ["a", "a"]
    else:
        source = "a" if case == "non_direct_binding" else "missing"
        if case in ("duplicate_binding", "literal_collision"):
            source = "b"
        binding = {"source_step_id": source, "target_argument": "records"}
        steps[2]["bindings"] = [binding]
        if case == "duplicate_binding":
            steps[2]["bindings"].append(binding.copy())
        elif case == "literal_collision":
            steps[2]["tool_call"]["arguments"]["records"] = []
    with pytest.raises(ValidationError):
        DAGPlan.model_validate({"mode": "dag", "steps": steps})


@pytest.mark.parametrize("field", ["source_step_id", "target_argument"])
def test_binding_rejects_blank_identifiers(field):
    values = {"source_step_id": "a", "target_argument": "records", field: " "}
    with pytest.raises(ValidationError):
        ResultBinding.model_validate(values)


def test_mutated_dag_is_revalidated():
    plan = DAGPlan(steps=[make_step("a"), make_step("b", ["a"])])
    plan.steps[0].depends_on.append("b")
    with pytest.raises(ValidationError):
        DAGPlan.model_validate(plan)


@pytest.mark.parametrize("field, value", [("id", ""), ("name", ""), ("arguments", {"bad": object()})])
def test_dag_step_revalidates_mutated_tool_call(field, value):
    call = ToolCall(id="source", name="probe", arguments={})
    setattr(call, field, value)
    with pytest.raises(ValidationError):
        DAGStep(tool_call=call)
