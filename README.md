# LLM Web Exploration with MCP (Multi-Capability Platform)

This script (`mcpTest.py`) demonstrates how to use an LLM (specifically Azure OpenAI's GPT models) to control a web browser for exploration tasks. It interacts with a locally running [Multi-Capability Platform (MCP)](https://github.com/AutonomousResearchGroup/mcp) server, which provides Playwright browser automation capabilities controllable via API calls.

## Functionality

*   **Connects** to an Azure OpenAI deployment.
*   **Establishes** an SSE (Server-Sent Events) connection with a local MCP server (expected at `http://localhost:8931`).
*   **Sends** user and system prompts to the LLM.
*   **Handles** function calls from the LLM, translating them into commands for the MCP server (e.g., navigate, click, type, snapshot).
*   **Sends** the results of MCP commands back to the LLM for the next action.
*   **Includes** robust error handling and reconnection logic for the MCP connection.
*   **Logs** interactions and potentially redirects output to a debug file.

## Prerequisites

*   **Python 3.8+**
*   **Node.js and npm/yarn:** Required for the MCP server.
*   **Azure OpenAI Account:** You need API credentials (key, endpoint, deployment name).
*   **MCP Server:** You need to have the MCP Playwright server running locally.

## Setup

1.  **Clone the MCP Repository:**
    ```bash
    git clone https://github.com/AutonomousResearchGroup/mcp.git
    cd mcp
    ```

2.  **Install MCP Dependencies:**
    ```bash
    npm install
    # or
    # yarn install
    ```

3.  **Install Playwright Browsers for MCP:**
    If you haven't already, install the necessary browsers for Playwright:
    ```bash
    npx playwright install --with-deps
    ```
    *(The `--with-deps` flag helps install necessary OS dependencies)*

4.  **Run the MCP Server:**
    The recommended way to run the MCP server is using `npx`, which ensures you're using the latest version. Open a terminal and run:
    ```bash
    npx @playwright/mcp@latest --port 8931 --headless
    ```
    *   `@playwright/mcp@latest`: Specifies the MCP package.
    *   `--port 8931`: Sets the port to match the script's expectation.
    *   `--headless`: Runs the browser without a visible UI (recommended for server environments or automation).

    *Alternative (if you cloned the repo in Step 1):*
    If you cloned the repository and installed dependencies (Steps 1 & 2), you can run it from the `mcp` directory:
    ```bash
    npm start
    # or
    # yarn start
    ```
    (Note: This might not include the `--headless` flag by default, and you might need to configure the port separately if it doesn't default to 8931).

5.  **Install Python Dependencies for this Script:**
    In the directory containing `mcpTest.py`, create a `requirements.txt` file with the following content:
    ```txt
    openai
    python-dotenv
    requests
    sseclient-py
    ```
    Then install them:
    ```bash
    pip install -r requirements.txt
    ```

6.  **Configure Environment Variables:**
    *   Copy the `.env.example` file to a new file named `.env`.
    *   Edit the `.env` file and replace the placeholder values with your actual Azure OpenAI credentials.
    ```dotenv
    # .env
    AZURE_OPENAI_KEY="YOUR_AZURE_OPENAI_KEY_HERE"
    AZURE_OPENAI_ENDPOINT="YOUR_AZURE_OPENAI_ENDPOINT_HERE"
    AZURE_OPENAI_API_VERSION="2023-09-01-preview"
    AZURE_OPENAI_DEPLOYMENT="YOUR_AZURE_DEPLOYMENT_NAME_HERE"
    ```

## Running the Script

1.  **Ensure the MCP server is running** (from Setup Step 4).
2.  **Modify Prompts:**
    *   Open `mcpTest.py`.
    *   Locate the `system_prompt` and `user_prompt` variables near the end of the file (within the `if __name__ == "__main__":` block).
    *   Replace the placeholder text within the triple quotes (`"""..."""`) with your desired system instructions and initial user request.
3.  **Run the script:**
    ```bash
    python mcpTest.py
    ```

4.  **Optional Debug Logging:**
    To save all console output to a file, modify the `run_mcp_request` call in `mcpTest.py` to include the `debug_file` argument:
    ```python
    result = run_mcp_request(
        prompt=user_prompt, 
        system_prompt=system_prompt,
        debug_file="mcp_run.log" # Add this line
    )
    ```

## How it Works (High Level)

The script initializes a `ClientRequest` object which:
1.  Starts a background thread (`_mcp_listener`) to connect to the MCP server's SSE endpoint and listen for messages (including the initial session ID).
2.  Waits for the listener to confirm initialization.
3.  Calls the `generate` method, which enters a loop:
    *   Sends the current conversation history (system prompt, user prompt, previous assistant/function calls/results) to the Azure OpenAI model.
    *   If the model responds with a function call request (e.g., `browser_click`):
        *   The script translates this to an MCP command (e.g., `tools/call` with `name: browser_click`).
        *   It sends the command to the MCP server via an HTTP POST request to the session's message URL.
        *   It waits for a response corresponding to the command ID via the SSE listener thread.
        *   The response (or error) is formatted and added back to the conversation history as a 'function' role message.
    *   If the model responds with text content, the loop breaks, and this content is returned as the final answer.
    *   The loop repeats, sending the updated history back to the model until it generates a final text response or hits an error/limit.
4.  Finally, the listener thread is stopped during cleanup. 