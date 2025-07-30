# pdf_chunks_util.py
import fitz  # PyMuPDF
import tempfile
from openai import OpenAI
import os
from dotenv import load_dotenv
import streamlit as st
load_dotenv()
#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
#client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
client = OpenAI()
CHUNK_SIZE = 50  # Pages per chunk

def split_pdf_to_chunks(file, chunk_size=CHUNK_SIZE):
    doc = fitz.open(stream=file.read(), filetype="pdf")
    total_pages = len(doc)
    chunks = []

    for start in range(0, total_pages, chunk_size):
        end = min(start + chunk_size, total_pages)
        chunk_doc = fitz.open()
        chunk_doc.insert_pdf(doc, from_page=start, to_page=end - 1)

        temp_path = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf").name
        chunk_doc.save(temp_path)
        chunk_doc.close()

        chunks.append((start + 1, end, temp_path))
    doc.close()
    return chunks
