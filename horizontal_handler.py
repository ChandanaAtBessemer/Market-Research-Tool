# horizontal_handler.py

import os
import time
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError
import streamlit as st
load_dotenv()
client = OpenAI()

PROMPT_ID = "pmpt_68890d096ab481968567c3d89d5e714c0ca0c19fe44835b6"
PROMPT_VERSION = "1"

TOOLS = [
    {"type": "web_search_preview"}
]


def get_horizontal_submarkets(industry: str, retries: int = 3) -> str:
    
    for attempt in range(retries):
        try:
            print(f"Fetching horizontals for {industry} [attempt {attempt+1}]")
            response = client.responses.create(
                prompt={
                    "id": PROMPT_ID,
                    "version": PROMPT_VERSION
                },
                input=industry,      # <-- just a string, not {"submarket": submarket}
                temperature=0.3
    
            )
            final = next((o for o in response.output if getattr(o, "type", "") == "message"), None)
            return "".join(part.text for part in final.content).strip() if final else "(no output)"
        except RateLimitError:
            delay = 3 + attempt * 2
            print(f" Rate limit. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            print(f"Error fetching horizontal submarkets: {e}")
            break
    return " Failed to retrieve horizontal sub-markets."

def main():
    st.title("horizontal Market")
    submarket = st.text_input("market", value="Plastics in Automotive")
    if st.button("Get Submarket Global Metrics"):
        with st.spinner("Fetching data..."):
            result = get_horizontal_submarkets(submarket)
        st.markdown(result)


if __name__ == "__main__":
    main()
