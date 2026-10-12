"""单次工具调用的 main 框架（Pydantic 2.x）。

用户输入 -> 模拟 LLM 选择工具 -> 校验参数 -> 模拟执行 -> 校验结果
         -> role=tool 回传 -> 模拟 LLM 生成最终回答。

三个 mock 函数只有固定数据，模块内部逻辑、真实 LLM 和真实查询均待实现。
安装依赖：python -m pip install "pydantic>=2,<3"
运行示例：python main.py
"""

import json
import logging
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

logger = logging.getLogger(__name__)
TOOL_NAME = "query_alarms"
DEMO_INPUT = "查询 node_20 昨天的严重告警"


class Contract(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


class ToolCall(Contract):
    """内部统一格式；后续由 LLM 适配器从响应中提取一次工具调用。"""

    id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    arguments: str  # 工具参数的 JSON 字符串，尚未校验。


class QueryAlarmsArgs(Contract):
    node_id: str = Field(pattern=r"^node_\d+$")
    time_range: Literal["yesterday"]
    severity: Literal["critical"]


class Alarm(Contract):
    alarm_id: str = Field(min_length=1)
    description: str = Field(min_length=1)


class QueryAlarmsData(Contract):
    alarms: list[Alarm]  # 空列表表示查询成功但没有告警，区别于查询失败。


def mock_llm_plan(messages: list[dict], tools: list[dict]) -> dict:
    """TODO：替换为 LLM 适配器；当前只模拟一次 query_alarms 调用。"""
    return {
        "id": "call_query_alarms_001",
        "name": TOOL_NAME,
        "arguments": json.dumps({
            "node_id": "node_20",
            "time_range": "yesterday",
            "severity": "critical",
        }),
    }


def mock_query_alarms(args: QueryAlarmsArgs) -> dict:
    """TODO：按参数查询告警；“昨天”按业务时区换算，查询及超时机制待实现。"""
    return {
        "alarms": [{
            "alarm_id": "demo_alarm_001",
            "description": "模拟告警：CPU 使用率超过严重告警阈值",
        }],
    }


def mock_llm_reply(messages: list[dict]) -> str:
    """TODO：将完整消息交给 LLM 总结，并禁止继续调用工具。"""
    payload = json.loads(messages[-1]["content"])
    if not payload["ok"]:
        return f"查询失败：{payload['error']['message']}"
    alarms = payload["data"]["alarms"]
    return f"【模拟结果】node_20 昨天共有 {len(alarms)} 条严重告警。"


def main(user_input: str = DEMO_INPUT) -> str:
    messages = [{"role": "user", "content": user_input}]
    tools = [{
        "type": "function",
        "function": {
            "name": TOOL_NAME,
            "description": "查询节点昨天的严重告警",
            "parameters": QueryAlarmsArgs.model_json_schema(),
        },
    }]

    # 1. LLM 请求和调用结构校验：失败时尚无可靠的 call_id，直接结束。
    try:
        raw_call = mock_llm_plan(messages, tools)
        call = ToolCall.model_validate(raw_call)
    except ValidationError:
        logger.exception("LLM 工具调用结构不合法")
        return "生成的工具调用结构不合法，未执行查询。"
    except (TimeoutError, ConnectionError):
        logger.exception("LLM 请求超时或连接失败")
        return "生成工具调用失败，请稍后重试。"
    except Exception:
        logger.exception("LLM 适配器发生未预期异常")
        return "生成工具调用时发生异常，未执行查询。"

    # 必须先保存 assistant 发起的调用，再追加与它对应的 tool 消息。
    messages.append({
        "role": "assistant",
        "content": None,
        "tool_calls": [{
            "id": call.id,
            "type": "function",
            "function": {"name": call.name, "arguments": call.arguments},
        }],
    })

    data = None
    error = None
    # 2. 工具名白名单：只允许一个查询工具，不能直接执行 LLM 给出的名字。
    if call.name != TOOL_NAME:
        error = {"code": "UNKNOWN_TOOL", "message": "请求的工具不受支持。"}
    else:
        # 3. JSON 解析与 Pydantic 参数校验；非法 JSON 也会抛出 ValidationError。
        try:
            args = QueryAlarmsArgs.model_validate_json(call.arguments)
        except ValidationError:
            logger.exception("工具参数校验失败")
            error = {"code": "INVALID_ARGUMENTS", "message": "查询参数不合法。"}
        else:
            # 4. 工具执行：适配器应配置真实超时，捕获异常本身不会终止阻塞。
            try:
                raw_data = mock_query_alarms(args)
            except TimeoutError:
                logger.exception("告警查询超时")
                error = {"code": "TOOL_TIMEOUT", "message": "告警查询超时。"}
            except ConnectionError:
                logger.exception("告警服务连接失败")
                error = {"code": "TOOL_UNAVAILABLE", "message": "告警服务暂不可用。"}
            except Exception:
                logger.exception("工具执行发生未预期异常")
                error = {"code": "TOOL_EXECUTION_ERROR", "message": "告警查询执行失败。"}
            else:
                # 5. 校验工具返回结构；不能将格式错误的结果当作查询成功。
                try:
                    result = QueryAlarmsData.model_validate(raw_data)
                    data = result.model_dump(mode="json")
                except ValidationError:
                    logger.exception("工具返回结果校验失败")
                    error = {"code": "INVALID_TOOL_RESULT", "message": "告警服务返回格式异常。"}

    # 6. 成功与失败都回传同一格式；异常详情仅记录日志，不原样传给 LLM。
    payload = {"ok": error is None, "data": data, "error": error}
    try:
        tool_content = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        logger.exception("工具结果序列化失败")
        tool_content = json.dumps({
            "ok": False,
            "data": None,
            "error": {"code": "SERIALIZATION_ERROR", "message": "工具结果无法序列化。"},
        }, ensure_ascii=False)

    messages.append({
        "role": "tool",
        "tool_call_id": call.id,  # 必须等于前面 assistant.tool_calls 中的 id。
        "content": tool_content,  # 工具结果是 JSON 字符串，不是 Python 对象。
    })

    # 7. 第二次模拟 LLM 只生成回答，不再触发工具；总结失败时保留工具结果。
    try:
        answer = mock_llm_reply(messages)
        if not isinstance(answer, str) or not answer.strip():
            raise ValueError("LLM 最终回答必须是非空字符串")
        return answer
    except Exception:
        logger.exception("LLM 结果总结失败")
        return f"结果总结失败，工具返回如下：\n{tool_content}"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(main())
