import subprocess
from typing import Annotated

from autogen_agentchat.agents import AssistantAgent
from autogen_agentchat.tools import AgentTool
from autogen_ext.models.openai import OpenAIChatCompletionClient


class LogAgent:
    def __init__(self, model_client: OpenAIChatCompletionClient):
        self.model_client = model_client
        self.agent = self.create_agent()

    def create_agent(self):
        log_agent = AssistantAgent(
            "log_analyst",
            model_client=self.model_client,
            system_message="You are a log analysis expert. Use the grep_search tool to search for patterns in log files and provide insights about what you find.",
            description="A log analysis expert that can search through files using grep.",
            tools=[self.grep_search],
            model_client_stream=True,
        )
        return AgentTool(log_agent, return_value_as_last_message=True)

    def grep_search(
        self,
        pattern: Annotated[str, "The search pattern to look for"],
        file_path: Annotated[str, "Path to the file to search in"],
        line_numbers: Annotated[bool, "Whether to show line numbers"] = True,
        ignore_case: Annotated[bool, "Whether to ignore case in search"] = False,
        context_lines: Annotated[
            int, "Number of context lines to show around matches"
        ] = 0,
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
                timeout=30,  # 30 second timeout
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
