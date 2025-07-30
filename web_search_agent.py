# web_search_agent.py

from openai import OpenAI
import os
from dotenv import load_dotenv
import streamlit as st
load_dotenv()
#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
client = OpenAI()

PROMPT_ID = "pmpt_688912c5d8cc8197b40a0409ce168ac2056afb650c14b3be"
PROMPT_VERSION = "1"


def search_web_insights(prompt: str) -> str:
    """
    Uses GPT-4 to generate market insights based on live web search.
    """
    
    try:
        response = client.responses.create(
            prompt={
                "id": PROMPT_ID,
                "version": PROMPT_VERSION
            },
            input=prompt,      # <-- just a string, not {"submarket": submarket}
            temperature=0.3

        )
        return response.output_text.strip()
    except Exception as e:
        return f" Web search failed: {e}"

def main():
    st.title("Web Search Agent")
    market = st.text_input("Market (e.g. Electric Vehicles)", value="Electric Vehicles")
    
    if st.button("Search"):
        with st.spinner("Fetching data..."):
            table_md = search_web_insights(market)
        st.markdown(table_md)


if __name__ == "__main__":
    main()
