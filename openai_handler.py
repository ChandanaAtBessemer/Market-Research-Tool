# openai_handler.py

import os
import time
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError
import streamlit as st
load_dotenv()
#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
#client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
client = OpenAI()
TOOLS = [
    {"type": "web_search_preview"}
]
PROMPT_ID = "pmpt_68890ae213f4819090dbf34cea8dc026040ce09fec32ba6f"
PROMPT_VERSION = "1"



def get_vertical_submarkets(market_query: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            print(f"Fetching verticals for {market_query} [attempt {attempt+1}]")
            response = client.responses.create(
                    prompt={
                        "id": PROMPT_ID,
                        "version": PROMPT_VERSION
                    },
                    input=market_query,      # <-- just a string, not {"submarket": submarket}
                    temperature = 0.3
            )
            final = next((o for o in response.output if getattr(o, "type", "") == "message"), None)
            return "".join(part.text for part in final.content).strip() if final else "(no output)"
        except RateLimitError:
            delay = 3 + attempt * 2
            print(f"⚠️ Rate limit. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            print(f" Error fetching vertical submarkets: {e}")
            break
    return "⚠️ Failed to retrieve vertical sub-markets."

def main():
    st.title("Vertical Segments Explorer")
    market = st.text_input("Market (e.g. Electric Vehicles)", value="Electric Vehicles")
    
    if st.button("Fetch Vertical Segments"):
        with st.spinner("Fetching data..."):
            table_md = get_vertical_submarkets(market)
        st.markdown(table_md)


if __name__ == "__main__":
    main()