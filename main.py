import subprocess
from typing import Annotated

import asyncio
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient
from autogen_agentchat.tools import AgentTool
from autogen_agentchat.ui import Console
from config import Config


def grep_search(
    pattern: Annotated[str, "The search pattern to look for"],
    file_path: Annotated[str, "Path to the file to search in"],
    line_numbers: Annotated[bool, "Whether to show line numbers"] = True,
    ignore_case: Annotated[bool, "Whether to ignore case in search"] = False,
    context_lines: Annotated[int, "Number of context lines to show around matches"] = 0,
) -> str:
    """Search for patterns in files using grep. Useful for log analysis and finding specific content."""
    try:
        # Build grep command
        cmd = ["grep"]
        
        # Add options
        if line_numbers:
            cmd.append("-n")
        if ignore_case:
            cmd.append("-i")
        if context_lines > 0:
            cmd.extend(["-C", str(context_lines)])
        
        # Add pattern and file
        cmd.extend([pattern, file_path])
        
        # Execute command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30  # 30 second timeout
        )
        
        if result.returncode == 0:
            return f"Found matches:\n{result.stdout}"
        elif result.returncode == 1:
            return f"No matches found for pattern '{pattern}' in file '{file_path}'"
        else:
            return f"Error running grep: {result.stderr}"
            
    except subprocess.TimeoutExpired:
        return "Grep command timed out (30 seconds)"
    except FileNotFoundError:
        return f"File not found: {file_path}"
    except Exception as e:
        return f"Error executing grep: {str(e)}"


async def main() -> None:
    model_client = OpenAIChatCompletionClient(**Config.COMPLETION_CLIENT_CONFIG)

    # Create log analysis agent with grep tool
    log_agent = AssistantAgent(
        "log_analyst",
        model_client=model_client,
        system_message="You are a log analysis expert. Use the grep_search tool to search for patterns in log files and provide insights about what you find.",
        description="A log analysis expert that can search through files using grep.",
        tools=[grep_search],
        model_client_stream=True,
    )
    log_agent_tool = AgentTool(log_agent, return_value_as_last_message=True)
    
    agent = AssistantAgent(
        "assistant",
        system_message="You are a general assistant. Use expert tools when needed. For log analysis or file searching tasks, use the log analyst.",
        model_client=model_client,
        model_client_stream=True,
        tools=[log_agent_tool],
        max_tool_iterations=10,
    )
    
    # Example queries - demonstrating log analysis capabilities
    await Console(agent.run_stream(task="Search for all ERROR entries in the file sample.log and summarize what types of errors occurred"))
    await Console(agent.run_stream(task="Find all PaymentService related entries in sample.log"))


asyncio.run(main())