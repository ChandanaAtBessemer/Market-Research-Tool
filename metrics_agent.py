# metrics_agent.py

import os
from openai import OpenAI
from dotenv import load_dotenv
import streamlit as st
load_dotenv()

client = OpenAI()
PROMPT_ID = "pmpt_6887def9d9a08195bb898ddc5bc4a12106162e31af023a7b"
PROMPT_VERSION = "1"

TOOLS = [{"type": "web_search_preview"}]

def get_detailed_metrics(submarket: str) -> str:
    user_query = f"Get the market size, CAGR, and forecast period for '{submarket}' market from 2018 to 2023 ."

    response = client.responses.create(
        prompt={
            "id": PROMPT_ID,
            "version": PROMPT_VERSION
        },
        input=submarket,      # <-- just a string, not {"submarket": submarket}
        temperature=0.3
    )


    for item in response.output:
        if getattr(item, "type", "") == "message":
            return "".join([c.text for c in item.content])

    return "❌ No output message from model."

def main():
    st.title("Metrics of Submarket")
    submarket = st.text_input("submarket", value="Plastics in Automotive")
    if st.button("Get Submarket Global Metrics"):
        with st.spinner("Fetching data..."):
            result = get_detailed_metrics(submarket)
        st.markdown(result)


if __name__ == "__main__":
    main()