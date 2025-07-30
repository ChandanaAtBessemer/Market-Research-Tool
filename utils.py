# utils.py

import re
import pandas as pd
from typing import Tuple
from io import StringIO
import re

def split_tables(markdown_text: str) -> tuple[str, str]:
    """
    Extracts the first and second Markdown tables found in the text.
    Assumes order: Vertical first, Horizontal second.
    """
    tables = re.findall(r"(?:\|.+\n)+\|[-| :]+\n(?:\|.+\n)+", markdown_text)
    if len(tables) >= 2:
        return tables[0], tables[1]
    elif len(tables) == 1:
        return tables[0], ""
    else:
        return "", ""
    #return match.group(1).strip(), match.group(2).strip()

def markdown_table_to_dataframe(md_table: str) -> pd.DataFrame:
    """
    Converts a single Markdown table string to a pandas DataFrame.
    """
    try:
        # Remove any stray bold, markdown styling
        clean = md_table.replace("**", "")
        return pd.read_csv(StringIO(clean), sep="|", engine="python").iloc[:, 1:-1].dropna(how="all")
    except Exception as e:
        print(f"⚠️ Failed to convert markdown to dataframe: {e}")
        return pd.DataFrame()

def extract_links(text: str) -> str:
    """
    Replaces markdown-style [text](url) with plain URL or just removes formatting.
    """
    return re.sub(r"\[(.*?)\]\((.*?)\)", r"\1 (URL: \2)", text)

def parse_markdown_table(md: str) -> pd.DataFrame:
    """
    Extracts the first markdown table and converts it to a pandas DataFrame.
    """
    try:
        clean_md = md.replace("**", "")
        table_match = re.search(r"((?:\|.+\n)+\|[-| :]+\n(?:\|.+\n)+)", clean_md)
        if not table_match:
            return pd.DataFrame()
        table_str = table_match.group(1)
        df = pd.read_csv(StringIO(table_str), sep="|", engine="python").iloc[:, 1:-1]
        return df.dropna(how="all")
    except Exception as e:
        print(f"⚠️ Table parsing error: {e}")
        return pd.DataFrame()