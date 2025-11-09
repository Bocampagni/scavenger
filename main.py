import asyncio

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.ui import Console
from autogen_ext.models.openai import OpenAIChatCompletionClient

from config import Config
from tools.log_agent import LogAgent


async def main() -> None:
    model_client = OpenAIChatCompletionClient(**Config.COMPLETION_CLIENT_CONFIG)
    log_agent_instance = LogAgent(model_client)

    orchestrator = AssistantAgent(
        "assistant",
        system_message="You are a general assistant. Use expert tools when needed. For log analysis or file searching tasks, use the log analyst.",
        model_client=model_client,
        model_client_stream=True,
        tools=[log_agent_instance.agent],
        max_tool_iterations=10,
    )

    await Console(
        orchestrator.run_stream(
            task="Search for all ERROR entries in the file sample.log and summarize what types of errors occurred"
        )
    )

asyncio.run(main())
