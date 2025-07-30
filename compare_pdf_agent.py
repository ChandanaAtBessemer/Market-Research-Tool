# compare_pdf_agent.py
import time
from openai import OpenAI
import os
from dotenv import load_dotenv
from pdf_chunks_util import split_pdf_to_chunks
import streamlit as st
load_dotenv()
client = OpenAI()

def compare_uploaded_pdfs(pdf_files: list, user_prompt: str) -> dict:
    results = {}

    for file in pdf_files:
        file_name = file.name
        chunks = split_pdf_to_chunks(file, chunk_size=50)
        chunk_outputs = []

        for (start, end, path) in chunks:
            with open(path, "rb") as f:
                uploaded = client.files.create(file=f, purpose="user_data")

            try:
                response = client.responses.create(
                    model="gpt-4o",
                    input=[
                        {
                            "role": "user",
                            "content": [
                                {"type": "input_file", "file_id": uploaded.id},
                                {"type": "input_text", "text": user_prompt}
                            ]
                        }
                    ]
                )
                chunk_outputs.append(f"**Pages {start}-{end}**\n{response.output_text.strip()}")
            except Exception as e:
                chunk_outputs.append(f" Error on pages {start}-{end}: {e}")

            time.sleep(1.5)  # Avoid rate limits

        results[file_name] = "\n\n".join(chunk_outputs)

    return results
