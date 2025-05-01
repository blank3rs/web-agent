from openai import AzureOpenAI
from dotenv import load_dotenv
import os
import requests
import json
import time
import uuid
import threading
from queue import Queue, Empty
import sseclient
import sys # Import sys for stream redirection

# Load environment variables
if not load_dotenv():
    print("Warning: .env file not found or couldn't be loaded")

class ClientRequest:
    def __init__(self, systemPrompt, model, userInput, streaming, temperature=0.7):
        self.systemPrompt = systemPrompt
        self.model = model
        self.userInput = userInput
        self.streaming = streaming
        self.temperature = temperature
        
        self.mcp_session_id = None
        self.mcp_message_url = None
        self.mcp_response_queue = Queue()
        self.mcp_listener_stop_event = threading.Event()
        self.mcp_init_complete_event = threading.Event()
        self.mcp_listener_thread = None

        # AzureOpenAI client configured from .env
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2023-09-01-preview"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.start_mcp_listener()
        
        print("[MCP Client] Waiting for MCP listener to initialize...")
        initialized = self.mcp_init_complete_event.wait(timeout=10) # Wait up to 10 seconds
        if not initialized:
            print("[MCP Client ERROR] MCP listener did not initialize in time.")
            self.stop_mcp_listener() # Clean up thread if init failed
            raise RuntimeError("MCP listener failed to initialize")
        else:
            print("[MCP Client] MCP listener initialized.")

    def _mcp_listener(self):
        """Background thread function to listen to MCP SSE endpoint."""
        sse_url = "http://localhost:8931/sse"
        reconnect_delay = 5 # Seconds to wait before retrying connection
        initial_connection_timeout = 60 # Increased timeout for initial connection

        while not self.mcp_listener_stop_event.is_set():
            try:
                print(f"[MCP Listener] Connecting to {sse_url}...")
                headers = {'Accept': 'text/event-stream'}
                # Use increased timeout for the connection attempt
                response = requests.get(sse_url, stream=True, headers=headers, timeout=initial_connection_timeout)
                response.raise_for_status() # Check for connection errors
                client = sseclient.SSEClient(response)
                print("[MCP Listener] Connected. Waiting for events...")
                
                # Flag to ensure init event is only set once per listener lifecycle
                init_signalled = self.mcp_init_complete_event.is_set()

                for event in client.events():
                    if self.mcp_listener_stop_event.is_set():
                        print("[MCP Listener] Stop event received during event processing.")
                        break # Exit inner event loop

                    if event.event == 'endpoint':
                        try:
                            # Check if data is the session URL string like /sse?sessionId=...
                            if isinstance(event.data, str) and event.data.startswith("/sse?sessionId="):
                                sse_url_data = event.data
                                # Extract session ID from the /sse URL
                                try:
                                    self.mcp_session_id = sse_url_data.split("sessionId=")[1].split("&")[0]
                                except IndexError:
                                    print(f"[MCP Listener ERROR] Could not parse session ID from SSE URL: {sse_url_data}")
                                    continue # Skip if URL format is wrong
                                
                                # Construct the message URL based on the extracted session ID
                                self.mcp_message_url = f"http://localhost:8931/message?sessionId={self.mcp_session_id}"
                                
                                print(f"[MCP Listener] Session Initialized. ID: {self.mcp_session_id}, Message URL: {self.mcp_message_url}")
                                if not init_signalled:
                                    self.mcp_init_complete_event.set() # Signal that initialization is done
                                    init_signalled = True
                            else:
                                # Handle unexpected format or potential JSON fallback
                                print(f"[MCP Listener WARN] Unexpected endpoint event data format or type: {type(event.data)}, {event.data}")
                                # Attempt JSON parsing as a fallback
                                try:
                                    data = json.loads(event.data)
                                    if isinstance(data, dict) and 'sessionId' in data:
                                        self.mcp_session_id = data['sessionId']
                                        self.mcp_message_url = f"http://localhost:8931/message?sessionId={self.mcp_session_id}"
                                        print(f"[MCP Listener] Session Initialized (Fallback JSON). ID: {self.mcp_session_id}, Message URL: {self.mcp_message_url}")
                                        if not init_signalled:
                                            self.mcp_init_complete_event.set()
                                            init_signalled = True
                                    else:
                                        print(f"[MCP Listener ERROR] Fallback JSON parsing did not yield session ID.")
                                except json.JSONDecodeError:
                                    print(f"[MCP Listener ERROR] Failed to parse endpoint data as expected string URL or fallback JSON.")
                        except Exception as e:
                            print(f"[MCP Listener ERROR] Error processing endpoint event: {e}")

                    elif event.event == 'message':
                        try:
                            message_data = json.loads(event.data)
                            # We expect JSON-RPC response format {jsonrpc, id, result/error}
                            message_id = message_data.get('id')
                            if message_id:
                                self.mcp_response_queue.put((message_id, message_data))
                            else:
                                print(f"[MCP Listener WARN] Received message without ID: {message_data}")
                        except json.JSONDecodeError:
                            print(f"[MCP Listener ERROR] Failed to parse message JSON: {event.data}")
                        except Exception as e:
                            print(f"[MCP Listener ERROR] Error processing message event: {e}")

                # If the event loop finishes normally (e.g., server closes connection gracefully),
                # check if we need to break the outer while loop based on stop_event
                if self.mcp_listener_stop_event.is_set():
                    print("[MCP Listener] Stop event detected after event loop.")
                    break # Exit outer while loop

            except requests.exceptions.RequestException as e:
                print(f"[MCP Listener ERROR] Connection failed: {e}. Retrying in {reconnect_delay} seconds...")
                # Signal init failure if connection fails before endpoint event *and* it hasn't been signalled successfully before
                if not self.mcp_init_complete_event.is_set():
                     print("[MCP Listener WARN] Initial connection failed. Will retry.")
                     # Setting the event here might cause the main thread to proceed thinking init failed permanently.
                     # Instead, we let the main thread's wait timeout handle initial failures if they persist.
                     # self.mcp_init_complete_event.set() # Avoid setting here? Let timeout handle it.

            except Exception as e:
                # Catch other potential exceptions during setup or event handling
                print(f"[MCP Listener ERROR] An unexpected error occurred in listener loop: {e}. Retrying in {reconnect_delay} seconds...")
                if not self.mcp_init_complete_event.is_set():
                    print("[MCP Listener WARN] Unexpected error before init completion. Will retry.")
                    # Similar logic as above, let the main thread timeout handle persistent startup issues.
                    # self.mcp_init_complete_event.set()

            # Wait before retrying connection if the loop didn't break due to stop_event
            if not self.mcp_listener_stop_event.is_set():
                print(f"[MCP Listener] Waiting {reconnect_delay}s before attempting reconnection.")
                # Use sleep_until for better interruptibility
                self.mcp_listener_stop_event.wait(timeout=reconnect_delay)


        print("[MCP Listener] Thread finished.")
        # Ensure init event is set if thread exits, especially if it never connected
        if not self.mcp_init_complete_event.is_set():
            print("[MCP Listener] Setting init complete event on final exit (likely indicating persistent failure).")
            self.mcp_init_complete_event.set()

    def start_mcp_listener(self):
        if self.mcp_listener_thread is None or not self.mcp_listener_thread.is_alive():
            self.mcp_listener_stop_event.clear()
            self.mcp_init_complete_event.clear()
            self.mcp_listener_thread = threading.Thread(target=self._mcp_listener, daemon=True)
            self.mcp_listener_thread.start()
            print("[MCP Client] Listener thread started.")
        else:
            print("[MCP Client] Listener thread already running.")

    def stop_mcp_listener(self):
        if self.mcp_listener_thread and self.mcp_listener_thread.is_alive():
            print("[MCP Client] Stopping listener thread...")
            self.mcp_listener_stop_event.set()
        self.mcp_listener_thread = None
        print("[MCP Client] Listener stop requested.") # Adjust log message

    def send_mcp_command(self, command, params=None):
        """Send a command to the MCP server via SSE message endpoint."""
        if not self.mcp_session_id or not self.mcp_message_url:
            error_msg = "MCP session not initialized. Cannot send command."
            print(f"[MCP Client ERROR] {error_msg}")
            return {"error": error_msg}

        # Map OpenAI function names to MCP tool names (use names directly from function definitions)
        # command will be 'navigate', 'snapshot', 'click', etc.
        tool_name = f"browser_{command}"
        
        # Handle snapshot parameters
        if command == "snapshot":
            params = {"random_string": "dummy"}
        
        # Prepare the message payload using JSON-RPC tools/call format
        request_id = str(uuid.uuid4())
        message_payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name, # Use the corrected tool_name
                "arguments": params or {}
            }
        }

        print(f"[MCP Client REQ] ID: {request_id}, Tool: {tool_name}, Params: {message_payload['params']['arguments']}")

        try:
            # Send the message POST request
            response = requests.post(
                self.mcp_message_url,
                json=message_payload,
                headers={'Content-Type': 'application/json'},
                timeout=10 # Add timeout
            )
            response.raise_for_status() # Check for immediate errors like 404 session not found

            # Wait for the response from the listener thread queue
            try:
                timeout = 120 # Increased from 60 to 120 seconds
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        # Check queue non-blockingly first
                        res_id, res_data = self.mcp_response_queue.get_nowait()
                        if res_id == request_id:
                            # Check for JSON-RPC error field within the message data
                            if 'error' in res_data:
                                print(f"[MCP Client JSON-RPC ERROR] {res_data['error']}")
                                return {"error": res_data['error'].get('message', json.dumps(res_data['error']))}
                            return res_data # Return the full JSON-RPC response
                        else:
                            # Put back messages not for us (should ideally not happen with unique IDs)
                            self.mcp_response_queue.put((res_id, res_data))
                            time.sleep(0.1) # Small delay before checking again
                    except Empty:
                        # Queue is empty, wait a bit before checking again
                        time.sleep(0.1)
                        if self.mcp_listener_stop_event.is_set() or not self.mcp_listener_thread.is_alive():
                            print("[MCP Client ERROR] Listener thread stopped while waiting for response.")
                            # Return a more specific error about session loss
                            return {"error": "MCP Connection/Session lost. Listener thread stopped."}
               
                # If loop finishes, timeout occurred
                print(f"[MCP Client ERROR] Timeout waiting for response for ID: {request_id}")
                return {"error": f"Timeout waiting for response (ID: {request_id})"}
                
            except Empty:
                print(f"[MCP Client ERROR] Timeout waiting for response for ID: {request_id}")
                 # Check if listener died during the wait
                if not self.mcp_listener_thread or not self.mcp_listener_thread.is_alive():
                     return {"error": f"MCP Connection/Session lost while waiting for response (ID: {request_id})"}
                else:
                     return {"error": f"Timeout waiting for response (ID: {request_id})"} # Corrected closing quote

        except requests.exceptions.RequestException as e:
            print(f"[MCP Client ERROR] Failed to send message: {e}")
            error_details = str(e)
            if e.response is not None:
                error_details += f"\nResponse: {e.response.text}"
            return {"error": error_details}
        except Exception as e:
            print(f"[MCP Client ERROR] Unexpected error sending command: {e}")
            return {"error": str(e)}

    def generate(self):
        messages = [
            {"role": "system", "content": self.systemPrompt},
            {"role": "user", "content": self.userInput}
        ]

        functions = [
            {
                "name": "browser_navigate",
                "description": "Navigate to a webpage.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "url": {"type": "string", "description": "URL to navigate to"}
                    },
                    "required": ["url"]
                }
            },
            {
                "name": "browser_snapshot",
                "description": "Capture accessibility snapshot of current page.",
                "parameters": { "type": "object", "properties": {} }
            },
            {
                "name": "browser_click",
                "description": "Click an element on the page.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "element": {"type": "string", "description": "Element description"},
                        "ref": {"type": "string", "description": "Element ref id"}
                    },
                    "required": ["element", "ref"]
                }
            },
            {
                "name": "browser_type",
                "description": "Type text into an editable element.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "element": {"type": "string", "description": "Human-readable element description"},
                        "ref": {"type": "string", "description": "Exact target element reference from the page snapshot"},
                        "text": {"type": "string", "description": "Text to type into the element"}
                        # Optional parameters can be added later if needed
                    },
                    "required": ["element", "ref", "text"]
                }
            },
            {
                 "name": "browser_tab_new",
                 "description": "Open a new browser tab, optionally navigating to a URL.",
                 "parameters": {
                     "type": "object",
                     "properties": {
                         "url": {"type": "string", "description": "Optional URL to navigate to in the new tab."}
                     },
                     "required": [] # URL is optional
                 }
            },
            {
                 "name": "browser_tab_select",
                 "description": "Select a browser tab by its index (0-based).",
                 "parameters": {
                     "type": "object",
                     "properties": {
                         "index": {"type": "integer", "description": "The 0-based index of the tab to select."}
                     },
                     "required": ["index"]
                 }
            },
            {
                 "name": "browser_tab_close",
                 "description": "Close a browser tab by its index (0-based). Closes current tab if index is not provided.",
                 "parameters": {
                     "type": "object",
                     "properties": {
                         "index": {"type": "integer", "description": "Optional 0-based index of the tab to close."}
                     },
                     "required": [] # Index is optional
                 }
            },
            {
                "name": "browser_press_key",
                "description": "Press a key on the keyboard (e.g., Enter, Escape, ArrowLeft, a).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "Name of the key to press or a character to generate, such as 'Enter' or 'a'."}
                    },
                    "required": ["key"]
                }
            },
            {
                "name": "browser_navigate_back",
                "description": "Navigate back to the previous page.",
                "parameters": {}
            }
        ]
        
        max_function_calls = 9999 # Limit loops to prevent infinite execution (Effectively removing limit)
        function_call_count = 0
        final_output = ""

        while function_call_count < max_function_calls:
            print(f"\n[LLM Call #{function_call_count + 1}] Sending messages to LLM...")
            # print(f"Messages: {json.dumps(messages, indent=2)}") # Debug messages
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=self.temperature,
                    max_tokens=8192, # Reduced max_tokens slightly
                    functions=functions,
                    function_call="auto"
                )
                
                choice = response.choices[0]
                message = choice.message

                # Check if the LLM wants to call a function
                if message.function_call:
                    function_call_count += 1
                    func_call = message.function_call
                    func_name = func_call.name
                    
                    # Ensure arguments are valid JSON, provide empty object if None/empty
                    try:
                        args_str = func_call.arguments if func_call.arguments else "{}"
                        args = json.loads(args_str)
                    except json.JSONDecodeError:
                        print(f"[ERROR] Invalid JSON arguments from LLM: {func_call.arguments}")
                        args = {} # Use empty args if parsing fails

                    print(f"\n[GPT Request] Function: {func_name}, Args: {args}")
                    
                    # Append the assistant's function call request message
                    messages.append({
                        "role": "assistant",
                        "content": None, # Content is null for function calls
                        "function_call": {
                            "name": func_name,
                            "arguments": func_call.arguments # Pass raw arguments string
                         }
                    })

                    # Execute the function via MCP
                    # Extract command part (e.g., 'navigate' from 'browser_navigate')
                    command = func_name.replace("browser_", "")
                    mcp_result = self.send_mcp_command(command, args)
                    
                    # Process MCP result for GPT
                    processed_content_for_gpt = None
                    content_to_dump = None
                    if isinstance(mcp_result, dict):
                        if 'error' in mcp_result:
                            error_data = mcp_result['error']
                            error_message = error_data.get('message', json.dumps(error_data)) if isinstance(error_data, dict) else json.dumps(error_data)
                            print(f"[MCP Client ERROR] {error_message}")
                            content_to_dump = {"error": error_message}
                        elif 'result' in mcp_result:
                            content_to_dump = mcp_result['result'] 
                        else:
                            print(f"[MCP Client WARN] Unexpected MCP response structure: {mcp_result}")
                            content_to_dump = mcp_result
                    else:
                        print(f"[MCP Client ERROR] Unexpected non-dict MCP response: {mcp_result}")
                        content_to_dump = {"error": f"Unexpected response type: {type(mcp_result)}"} # Prepare error dict
                    
                    # --- Pretty-print the dictionary to the log file ---
                    print("[MCP RESPONSE For GPT - Formatted]:") # Commented out as requested
                    try:
                         print(json.dumps(content_to_dump, indent=2)) # Pretty print the dictionary # Commented out as requested
                    except Exception as dump_error:
                         print(f"  (Error pretty-printing: {dump_error})\n  Raw content: {content_to_dump}")
                    # -----------------------------------------------------
                        
                    # Now dump the prepared content to JSON string for the LLM
                    processed_content_for_gpt = json.dumps(content_to_dump)
                    
                    # print(f"[MCP RESPONSE For GPT - Raw String for LLM]: {processed_content_for_gpt}") # Keep raw string log too # Commented out as requested

                    # Feed processed MCP result back to GPT as function output
                    messages.append({
                        "role": "function", 
                        "name": func_name, 
                        "content": processed_content_for_gpt
                    })
                    
                else:
                    # No function call, LLM provided a text response
                    final_output = message.content
                    print(f"\n[Final GPT Response]\n{final_output}")
                    break # Exit the loop

            except Exception as e:
                print(f"Error during LLM call or processing: {e}")
                final_output = f"An error occurred: {e}"
                break # Exit loop on error
        
        if function_call_count >= max_function_calls:
             print("\n[WARN] Maximum function call limit reached.")
             final_output = "Maximum function call limit reached. Unable to complete the request fully."

        return final_output

def run_mcp_request(prompt: str, system_prompt: str = "You are a helpful assistant controlling a browser.", debug_file: str | None = None, streaming: bool = False):
    """
    Runs a request through the MCP Client, handling setup, execution, and cleanup.

    Args:
        prompt: The user input prompt for the LLM.
        system_prompt: The system prompt for the LLM.
        debug_file: Optional path to a file for debug logging. If provided, stdout/stderr will be redirected.
        streaming: Whether to use streaming mode (currently affects ClientRequest initialization).

    Returns:
        The final output from the LLM or an error message.
    """
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    log_file = None
    testObject = None
    final_result = None

    try:
        # --- Output Redirection Setup ---
        if debug_file:
            print(f"Redirecting output to {debug_file}...", file=original_stdout) # Print to original stdout
            log_file = open(debug_file, 'a', encoding='utf-8')
            sys.stdout = log_file
            sys.stderr = log_file
            print(f"\n--- New Run Started: {time.strftime('%Y-%m-%d %H:%M:%S')} ---") # Add timestamp
        # --------------------------------

        try:
            model_name = os.getenv("AZURE_OPENAI_DEPLOYMENT")
            if not model_name:
                raise ValueError("AZURE_OPENAI_DEPLOYMENT environment variable not set.")

            testObject = ClientRequest(
                system_prompt,
                model_name,
                prompt,
                streaming=streaming # Pass streaming argument
            )
            final_result = testObject.generate()
        except ValueError as ve:
             print(f"[SETUP ERROR] {ve}")
             final_result = f"Setup Error: {ve}"
        except RuntimeError as e:
            print(f"Failed to run test: {e}")
            final_result = f"Runtime Error: {e}"
        except Exception as e_main:
             print(f"An unexpected error occurred during execution: {e_main}") # Log other exceptions
             final_result = f"Unexpected Error: {e_main}"
        finally:
            if testObject:
                 print("[Cleanup] Stopping MCP listener...")
                 testObject.stop_mcp_listener() # Ensure cleanup

    finally:
        # --- Restore Output Streams ---
        if log_file:
            print(f"--- Run Finished: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
            sys.stdout = original_stdout
            sys.stderr = original_stderr
            log_file.close()
            print(f"Output logging to {debug_file} complete.", file=original_stdout) # Print to original stdout
        elif testObject: # If no log file, but object existed, log listener stop completion to original stdout
             print("[Cleanup] MCP listener stopped.", file=original_stdout)
        # -----------------------------
    
    return final_result


if __name__ == "__main__":
    # === Configuration ===
    # Define your system prompt (instructions for the LLM)
    system_prompt = """
    --- PLACE YOUR SYSTEM PROMPT HERE --- 
    
    Example: 
    You are a helpful assistant controlling a browser. 
    1. Navigate to the requested URL.
    2. Find the contact information.
    3. Report the contact information back.
    """
    
    # Define the initial user request
    user_prompt = """
    --- PLACE YOUR USER PROMPT HERE --- 
    
    Example:
    Go to example.com and find the email address listed on their contact page.
    """
    # === End Configuration ===
    
    # Optional: Specify a file to log all output
    # debug_log_file = "mcp_run.log"
    debug_log_file = None 

    # Run the request
    result = run_mcp_request(
        prompt=user_prompt, 
        system_prompt=system_prompt, 
        debug_file=debug_log_file 
    )

    # Print the final result to the console 
    print("\n=== FINAL RESULT ===")
    print(result)


