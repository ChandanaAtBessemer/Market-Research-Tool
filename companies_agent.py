import os
from openai import OpenAI
from dotenv import load_dotenv
import streamlit as st

# Load environment variables (ensure OPENAI_API_KEY is set)
load_dotenv()
client = OpenAI()

# ID and version of your stored prompt template
PROMPT_ID = "pmpt_68842d6c0b448196a868674711e6639409c9f231eee31359"
PROMPT_VERSION = "2"

def get_top_companies(submarket: str) -> str:
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
            return "".join(c.text for c in item.content)

    return "❌ No output from GPT-4o."



def main():
    st.title("Top Companies by Submarket")
    submarket = st.text_input("Submarket", value="Plastics in Automotive")
    if st.button("Get Top Companies"):
        with st.spinner("Fetching data..."):
            result = get_top_companies(submarket)
        st.markdown(result)


if __name__ == "__main__":
    main()
