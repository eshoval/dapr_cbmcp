# appv2.py (Corrected version)

import os
import chainlit as cl
from dapr_agents import Agent
from dapr_agents.tool.mcp.client import MCPClient
from dapr_agents.llm.dapr import DaprChatClient
from dapr_agents.types import LLMChatResponse, UserMessage
from dotenv import load_dotenv

load_dotenv()

# --- Agent Instructions  ---
instructions = [
    "You are an expert N1QL (Couchbase) query specialist with 10+ years of experience.",
    "ROLE: Senior N1QL Database Engineer specializing in JSON document querying and optimization.",
    "SESSION BEHAVIOR: Use schema discovery tools only during initialization. For query generation, rely on the provided schema context.",
    "OPTIMIZATION FOCUS: Generate syntactically correct, performance-optimized SQL++ queries using the established schema knowledge."
]

@cl.on_chat_start
async def start():
    """
    Initializes the agent when a new chat session starts.
    """
    
     # ;loading a pre created data schema of the Couchbase content. It is created with cb_discovery.py
    try:
        with open('schema_context.json', 'r', encoding='utf-8') as f:
            schema_context = f.read()
    except FileNotFoundError:
        await cl.Message(content="Error: `schema_context.json` not found. Please run `cb_discovery.py` script first to generate it.").send()
        return
    cl.user_session.set("schema_context", schema_context)

    # --- שלב 2: התחברות ל-MCP ויצירת Agent של Dapr ---
    mcp_url = os.getenv("MCP_SERVER_URL")
    if not mcp_url:
        await cl.Message(content="Error: MCP_SERVER_URL environment variable not set.").send()
        return    
    client = MCPClient(timeout=60.0)
    try:
        await client.connect_sse(server_name="couchbase_mcp", url=mcp_url, headers=None)
    except Exception as e:
        await cl.Message(content=f"Failed to connect to MCP Server: {e}").send()
        return
        
    tools = client.get_all_tools()
    # Create the Agent 
    component_name = os.getenv("DAPR_LLM_COMPONENT_DEFAULT", "openai")
    agent = Agent(
        name="TestAgent",
        role="software architect and expert in Dapr and Dapr agents",
        instructions=instructions,
        llm=DaprChatClient(component_name=component_name, enable_tool_calls=True),
        tools=tools,     # When I Uncomment it to use MCP tools the agent crashes because he invokes openai with DAPR_LLM_TOOL_FORMAT = dapr 
    #    when i use it without tools the agent uses the DAPR_LLM_TOOL_FORMAT from the env variable - opneai
    )

    cl.user_session.set("agent", agent)

    # Send a ready message to the user
    await cl.Message(
        content="✅ Couchbase Agent is ready. How can I help?"
    ).send()

@cl.on_message
async def main(message: cl.Message):
    """
    Handles incoming user messages.
    """
    agent = cl.user_session.get("agent")
    schema_context = cl.user_session.get("schema_context")    
    prompt = message.content

    try:
        final_result = await agent.run(prompt)
        
        # Handle different response types
        if hasattr(final_result, 'content'):
            response_content = final_result.content
        elif hasattr(final_result, 'message') and hasattr(final_result.message, 'content'):
            response_content = final_result.message.content
        else:
            response_content = str(final_result)

        await cl.Message(
            content=response_content,
        ).send()
    except Exception as e:
        print(f"Debug - final_result type: {type(final_result)}")
        print(f"Debug - final_result attributes: {dir(final_result)}")
        await cl.Message(
            content=f"Error: {str(e)}"
        ).send()

