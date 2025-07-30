# global_metrics_agent.py

import os
import time
from dotenv import load_dotenv
from openai import OpenAI, RateLimitError
import streamlit as st
load_dotenv()
#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
#client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
client = OpenAI()

TOOLS = [{"type": "web_search_preview"}]

PROMPT_ID = "pmpt_6887dbd520548196b288b61816a837a901c90dc46b715c8f"
PROMPT_VERSION = "1"



def get_global_overview(market: str, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            print(f"Fetching global metrics for {market} (attempt {attempt+1})")
            response = client.responses.create(
                prompt={
                        "id": PROMPT_ID,
                        "version": PROMPT_VERSION
                        },
                input=market,      # <-- just a string, not {"submarket": submarket}
                temperature=0.3
            )
            final = next((o for o in response.output if getattr(o, "type", "") == "message"), None)
            return "".join(part.text for part in final.content).strip() if final else "⚠️ No output returned."
        except RateLimitError:
            delay = 3 + attempt * 2
            print(f"⚠️ Rate limit hit. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            print(f" Error: {e}")
            break
    return "⚠️ Failed to fetch global overview."

def main():
    st.title("Global Metrics of Market")
    market = st.text_input("market", value="Plastics in Automotive")
    if st.button("Get Global Metrics"):
        with st.spinner("Fetching data..."):
            result = get_global_overview(market)
        st.markdown(result)


if __name__ == "__main__":
    main()