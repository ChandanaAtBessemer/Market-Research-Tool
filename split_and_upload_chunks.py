import fitz  # PyMuPDF
import tempfile
from openai import OpenAI
import os
import streamlit as st
#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
#client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

client = OpenAI()
CHUNK_SIZE = 50

def split_and_upload_pdf_chunks(file_stream) -> list:
    doc = fitz.open(stream=file_stream.read(), filetype="pdf")
    total_pages = len(doc)
    file_id_chunks = []

    for start in range(0, total_pages, CHUNK_SIZE):
        end = min(start + CHUNK_SIZE, total_pages)
        chunk_doc = fitz.open()
        chunk_doc.insert_pdf(doc, from_page=start, to_page=end - 1)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            chunk_doc.save(tmp.name)
            uploaded = client.files.create(file=open(tmp.name, "rb"), purpose="user_data")
            file_id_chunks.append({"file_id": uploaded.id, "start": start + 1, "end": end})

        chunk_doc.close()

    doc.close()
    return file_id_chunks
