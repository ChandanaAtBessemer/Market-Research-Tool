import os
import time
from openai import OpenAI, RateLimitError
from dotenv import load_dotenv
import streamlit as st

# Load your API key
load_dotenv()
client = OpenAI()

# Stored prompt reference
PROMPT_ID = "pmpt_6887e2f23c9c81959d041e23c50f22d8024bea49ae171cac"
PROMPT_VERSION = "1"
TOOLS = [{"type": "web_search_preview"}]


def get_mergers_table(market: str, timeframe: str, retries: int = 3) -> str:
    """
    Retrieves an M&A deals table for the given market and timeframe using a saved prompt template.
    Returns the result as a Markdown table string.
    """
    prompt_ref = {"id": PROMPT_ID, "version": PROMPT_VERSION}
    # Build a single user message with both inputs as content items
    user_message = {
        "role": "user",
        "content": [
            {"type": "input_text", "text": market},
            {"type": "input_text", "text": timeframe}
        ]
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"🔍 Fetching M&A data for '{market}' in '{timeframe}' (attempt {attempt})")
            response = client.responses.create(
                prompt=prompt_ref,
                input=[user_message],
                tools=TOOLS,
                temperature=0.3
            )
            # Extract the assistant message
            message = next((o for o in response.output if getattr(o, "type", "") == "message"), None)
            if message:
                return "".join(part.text for part in message.content).strip()
            return "⚠️ No output returned."

        except RateLimitError:
            delay = 2 * attempt
            print(f"⚠️ Rate limit hit, retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            print(f"❌ Error on attempt {attempt}: {e}")
            break

    return "⚠️ Failed to retrieve M&A data after retries."


def main():
    st.title("Mergers & Acquisitions Explorer")
    market = st.text_input("Market (e.g. Electric Vehicles)", value="Electric Vehicles")
    timeframe = st.text_input("Timeframe (e.g. last 5 years)", value="last 5 years")
    if st.button("Fetch M&A Table"):
        with st.spinner("Fetching data..."):
            table_md = get_mergers_table(market, timeframe)
        st.markdown(table_md)


if __name__ == "__main__":
    main()
