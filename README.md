# Scavenger - Agentic Log Retrieval POC

A proof-of-concept for intelligent log retrieval using multi-agent systems powered by AutoGen.

## Overview

This project demonstrates agentic log retrieval capabilities using specialized AI agents that can understand and process different types of technical queries through expert knowledge domains.

## Features

- Streaming conversation interface
- OpenAI/OpenRouter integration
- Tool-based agent interactions

## Setup

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Configure environment variables in `.env`:
   ```
   OPENAI_API_KEY=your_api_key_here
   MODEL=gpt-4o-mini-2024-07-18
   BASE_URL=https://openrouter.ai/api/v1
   ```

3. Run the application:
   ```bash
   python3 scavenger.py
   ```

