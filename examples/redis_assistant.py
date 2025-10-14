import asyncio
from agents import Agent, Runner
from openai.types.responses import ResponseTextDeltaEvent
from agents.mcp import MCPServerStdio
from collections import deque


# Set up and create the agent
async def build_agent():
    # Redis MCP Server. Pass the environment configuration for the MCP Server in the JSON
    server = MCPServerStdio(
        params={
            "command": "uv",
            "args": [
                "--directory",
                "../src/",  # change with the path to the MCP server
                "run",
                "main.py",
            ],
            "env": {
                "REDIS_HOST": "127.0.0.1",
                "REDIS_PORT": "6379",
                "REDIS_USERNAME": "default",
                "REDIS_PWD": "",
            },
        }
    )

    await server.connect()

    # Create and return the agent
    agent = Agent(
        name="Redis Assistant",
        instructions="You are a helpful assistant capable of reading and writing to Redis. Store every question and answer in the Redis Stream app:logger",
        mcp_servers=[server],
    )

    return agent


# CLI interaction
async def cli(agent, max_history=30):
    print("🔧 Redis Assistant CLI — Ask me something (type 'exit' to quit):\n")
    conversation_history = deque(maxlen=max_history)

    while True:
        q = input("❓> ")
        if q.strip().lower() in {"exit", "quit"}:
            break

        if len(q.strip()) > 0:
            # Format the context into a single string
            history = ""
            for turn in conversation_history:
                prefix = "User" if turn["role"] == "user" else "Assistant"
                history += f"{prefix}: {turn['content']}\n"

            context = f"Conversation history:/n{history.strip()} /n New question:/n{q.strip()}"
            result = Runner.run_streamed(agent, context)

            response_text = ""
            async for event in result.stream_events():
                if event.type == "raw_response_event" and isinstance(
                    event.data, ResponseTextDeltaEvent
                ):
                    print(event.data.delta, end="", flush=True)
                    response_text += event.data.delta
            print("\n")

            # Add the user's message and the assistant's reply in history
            conversation_history.append({"role": "user", "content": q})
            conversation_history.append({"role": "assistant", "content": response_text})


# Main entry point
async def main():
    agent = await build_agent()
    await cli(agent)


if __name__ == "__main__":
    asyncio.run(main())
