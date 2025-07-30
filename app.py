

# working_enhanced_app.py - Your full app with working database

import streamlit as st
import hashlib
import json
import sqlite3
import os
from datetime import datetime
from typing import Optional, List, Dict, Any
import pandas as pd
# Import your existing modules
from openai_handler import get_vertical_submarkets
from horizontal_handler import get_horizontal_submarkets
from global_metrics_agent import get_global_overview
from utils import split_tables, markdown_table_to_dataframe
from metrics_agent import get_detailed_metrics
from companies_agent import get_top_companies
from mergers_agent import get_mergers_table
from split_and_upload_chunks import split_and_upload_pdf_chunks
from query_uploaded_chunks import query_chunks
from compare_pdf_agent import compare_uploaded_pdfs
from web_search_agent import search_web_insights

st.set_page_config(page_title="Market Sub-Segment Explorer", layout="wide")

# === WORKING DATABASE CLASS (BUILT-IN) ===
class WorkingMarketDB:
    def __init__(self, db_path: str = "working_market.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS market_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_name TEXT NOT NULL,
                    query_type TEXT NOT NULL,
                    query_hash TEXT UNIQUE NOT NULL,
                    result_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    source TEXT DEFAULT 'openai'
                );
                
                CREATE TABLE IF NOT EXISTS pdf_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    file_size INTEGER,
                    total_pages INTEGER,
                    chunks_count INTEGER,
                    openai_file_ids TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'processed'
                );
                
                CREATE TABLE IF NOT EXISTS pdf_qa (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pdf_history_id INTEGER,
                    question TEXT NOT NULL,
                    answer TEXT,
                    query_tokens INTEGER,
                    response_tokens INTEGER,
                    cost_estimate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (pdf_history_id) REFERENCES pdf_history (id)
                );
                
                CREATE TABLE IF NOT EXISTS ma_searches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    market_name TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    result_data TEXT,
                    deals_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS usage_analytics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    event_data TEXT,
                    user_agent TEXT,
                    session_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_market_cache_hash ON market_cache(query_hash);
                CREATE INDEX IF NOT EXISTS idx_market_cache_name ON market_cache(market_name);
                CREATE INDEX IF NOT EXISTS idx_pdf_history_hash ON pdf_history(file_hash);
            """)
    
    def generate_query_hash(self, market_name: str, query_type: str, **kwargs) -> str:
        """Generate a hash for caching purposes"""
        query_string = f"{market_name}:{query_type}:{json.dumps(kwargs, sort_keys=True)}"
        return hashlib.md5(query_string.encode()).hexdigest()
    
    # === CACHE METHODS ===
    def get_cached_result(self, market_name: str, query_type: str, **kwargs) -> Optional[Dict]:
        """Retrieve cached market analysis result"""
        query_hash = self.generate_query_hash(market_name, query_type, **kwargs)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT result_data, created_at, expires_at 
                FROM market_cache 
                WHERE query_hash = ? AND (expires_at IS NULL OR expires_at > datetime('now'))
            """, (query_hash,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'data': row['result_data'],  # Keep as string for simplicity
                    'cached_at': row['created_at'],
                    'expires_at': row['expires_at']
                }
        return None
    
    def cache_result(self, market_name: str, query_type: str, result_data: Any, 
                    source: str = 'openai', expire_hours: int = 24, **kwargs):
        """Cache a market analysis result"""
        query_hash = self.generate_query_hash(market_name, query_type, **kwargs)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO market_cache 
                (market_name, query_type, query_hash, result_data, source, expires_at)
                VALUES (?, ?, ?, ?, ?, datetime('now', '+{} hours'))
            """.format(expire_hours), (market_name, query_type, query_hash, 
                                     str(result_data), source))
    
    # === PDF METHODS ===
    def save_pdf_processing(self, file_name: str, file_content: bytes, 
                           total_pages: int, openai_file_ids: List[str]) -> int:
        """Save PDF processing information"""
        file_hash = hashlib.md5(file_content).hexdigest()
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO pdf_history 
                (file_name, file_hash, file_size, total_pages, chunks_count, openai_file_ids)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (file_name, file_hash, len(file_content), total_pages, 
                  len(openai_file_ids), json.dumps(openai_file_ids)))
            
            return cursor.lastrowid
    
    def get_pdf_by_hash(self, file_hash: str) -> Optional[Dict]:
        """Check if PDF was already processed"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM pdf_history WHERE file_hash = ? AND status = 'processed'
                ORDER BY processed_at DESC LIMIT 1
            """, (file_hash,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row['id'],
                    'file_name': row['file_name'],
                    'total_pages': row['total_pages'],
                    'chunks_count': row['chunks_count'],
                    'openai_file_ids': json.loads(row['openai_file_ids']),
                    'processed_at': row['processed_at']
                }
        return None
    
    def save_pdf_qa(self, pdf_history_id: int, question: str, answer: str, 
                   query_tokens: int = 0, response_tokens: int = 0) -> int:
        """Save PDF Q&A interaction"""
        cost_estimate = (query_tokens * 0.01 + response_tokens * 0.03) / 1000
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO pdf_qa 
                (pdf_history_id, question, answer, query_tokens, response_tokens, cost_estimate)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (pdf_history_id, question, answer, query_tokens, response_tokens, cost_estimate))
            
            return cursor.lastrowid
    
    def get_pdf_qa_history(self, pdf_history_id: int) -> List[Dict]:
        """Get Q&A history for a specific PDF"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT question, answer, created_at, cost_estimate
                FROM pdf_qa 
                WHERE pdf_history_id = ?
                ORDER BY created_at DESC
            """, (pdf_history_id,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # === M&A METHODS ===
    def save_ma_search(self, market_name: str, timeframe: str, result_data: str, deals_count: int = 0):
        """Save M&A search result"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO ma_searches (market_name, timeframe, result_data, deals_count)
                VALUES (?, ?, ?, ?)
            """, (market_name, timeframe, result_data, deals_count))
    
    def get_recent_ma_searches(self, limit: int = 10) -> List[Dict]:
        """Get recent M&A searches"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT market_name, timeframe, deals_count, created_at, result_data
                FROM ma_searches 
                ORDER BY created_at DESC 
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    # === ANALYTICS METHODS ===
    def log_event(self, event_type: str, event_data: Dict = None, session_id: str = None):
        """Log usage analytics event"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO usage_analytics (event_type, event_data, session_id)
                VALUES (?, ?, ?)
            """, (event_type, json.dumps(event_data) if event_data else None, session_id))
    
    def get_popular_markets(self, days: int = 30, limit: int = 10) -> List[Dict]:
        """Get most popular markets in the last N days"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT market_name, COUNT(*) as query_count, MAX(created_at) as last_queried
                FROM market_cache 
                WHERE created_at > datetime('now', '-{} days')
                GROUP BY market_name
                ORDER BY query_count DESC
                LIMIT ?
            """.format(days), (limit,))
            
            return [{'market_name': row[0], 'query_count': row[1], 'last_queried': row[2]} 
                   for row in cursor.fetchall()]
    
    # === HISTORY BROWSING METHODS ===
    def get_market_analysis_history(self, limit: int = 20) -> List[Dict]:
        """Get history of market analyses for browsing"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT DISTINCT market_name, query_type, created_at,
                       COUNT(*) OVER (PARTITION BY market_name, query_type) as access_count
                FROM market_cache 
                WHERE expires_at > datetime('now') OR expires_at IS NULL
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def get_pdf_sessions_summary(self, limit: int = 15) -> List[Dict]:
        """Get summary of PDF sessions for browsing"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT p.id, p.file_name, p.total_pages, p.chunks_count, 
                       p.processed_at, p.openai_file_ids,
                       COUNT(q.id) as qa_count,
                       MAX(q.created_at) as last_question
                FROM pdf_history p
                LEFT JOIN pdf_qa q ON p.id = q.pdf_history_id
                WHERE p.status = 'processed'
                GROUP BY p.id
                ORDER BY p.processed_at DESC
                LIMIT ?
            """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
    
    def restore_pdf_session(self, pdf_id: int) -> Dict:
        """Get all data needed to restore a PDF session"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get PDF info
            pdf_cursor = conn.execute("""
                SELECT * FROM pdf_history WHERE id = ? AND status = 'processed'
            """, (pdf_id,))
            pdf_data = pdf_cursor.fetchone()
            
            if not pdf_data:
                return None
            
            # Get Q&A history
            qa_cursor = conn.execute("""
                SELECT question, answer, created_at, cost_estimate
                FROM pdf_qa 
                WHERE pdf_history_id = ?
                ORDER BY created_at ASC
            """, (pdf_id,))
            qa_history = [dict(row) for row in qa_cursor.fetchall()]
            
            return {
                'pdf_info': dict(pdf_data),
                'qa_history': qa_history
            }
    
    # === UTILITY METHODS ===
    def cleanup_expired_cache(self):
        """Remove expired cache entries"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM market_cache WHERE expires_at < datetime('now')")
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            tables = ['market_cache', 'pdf_history', 'pdf_qa', 'ma_searches', 'usage_analytics']
            for table in tables:
                cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                stats[f"{table}_count"] = cursor.fetchone()[0]
            
            stats['db_size_mb'] = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            return stats

# === INITIALIZE APP ===
# Clear any cached resources to prevent conflicts
st.cache_data.clear()
st.cache_resource.clear()

# Initialize database
@st.cache_resource
def init_working_database():
    """Initialize working database connection"""
    return WorkingMarketDB()

db = init_working_database()

# Initialize session state variables
def initialize_session_state():
    """Initialize all session state variables if they don't exist"""
    session_vars = {
        "global_md": None,
        "raw_markdown": None,
        "vertical_df": None,
        "horizontal_df": None,
        "market_analyzed": None,
        "suggested_market": "",
        "pdf_file_id_chunks": [],
        "pdf_responses": [],
        "uploaded_pdf_name": None,
        "current_pdf_id": None,
        "ma_results": None,
        "ma_market_searched": None,
        "suggested_ma_market": "",
        "comparison_results": None,
        "web_insights_results": None,
        "compared_files": [],
        "show_popular_markets": True,
        "show_recent_ma": True
    }
    
    for key, default_value in session_vars.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

initialize_session_state()

# Generate session ID for analytics
if 'session_id' not in st.session_state:
    st.session_state.session_id = hashlib.md5(str(st.session_state).encode()).hexdigest()[:12]

# === ENHANCED FUNCTIONS ===
@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_cached_market_analysis(market_name: str, analysis_type: str):
    """Get market analysis with caching"""
    # Check cache first
    cached = db.get_cached_result(market_name, analysis_type)
    if cached:
        return cached['data'], True  # Return data and cache_hit flag
    
    # Perform fresh analysis
    if analysis_type == 'global':
        result = get_global_overview(market_name)
    elif analysis_type == 'vertical':
        result = get_vertical_submarkets(market_name)
    elif analysis_type == 'horizontal':
        result = get_horizontal_submarkets(market_name)
    else:
        result = None
    
    # Cache the result for 24 hours
    if result:
        db.cache_result(market_name, analysis_type, result, expire_hours=24)
        
        # Log analytics event
        db.log_event('market_analysis', {
            'market_name': market_name,
            'analysis_type': analysis_type,
            'result_length': len(str(result))
        }, st.session_state.session_id)
    
    return result, False  # Return data and cache_hit flag

def process_pdf_with_deduplication(uploaded_file):
    """Process PDF with deduplication check"""
    # Read file content for hashing
    file_content = uploaded_file.read()
    file_hash = hashlib.md5(file_content).hexdigest()
    
    # Check if already processed
    existing_pdf = db.get_pdf_by_hash(file_hash)
    if existing_pdf:
        st.success(f"📋 PDF '{existing_pdf['file_name']}' already processed! Using existing {existing_pdf['chunks_count']} chunks.")
        
        # Convert back to the format expected by your app
        file_chunks = []
        openai_file_ids = existing_pdf['openai_file_ids']
        pages_per_chunk = existing_pdf['total_pages'] // existing_pdf['chunks_count']
        
        for i, file_id in enumerate(openai_file_ids):
            start_page = i * pages_per_chunk + 1
            end_page = min((i + 1) * pages_per_chunk, existing_pdf['total_pages'])
            file_chunks.append({
                'file_id': file_id,
                'start': start_page,
                'end': end_page
            })
        
        st.session_state.current_pdf_id = existing_pdf['id']
        return file_chunks
    
    # Process new PDF
    uploaded_file.seek(0)  # Reset file pointer
    with st.spinner("⏳ Processing new PDF..."):
        file_chunks = split_and_upload_pdf_chunks(uploaded_file)
        openai_file_ids = [chunk['file_id'] for chunk in file_chunks]
        total_pages = max([chunk['end'] for chunk in file_chunks])
        
        # Save to database
        pdf_id = db.save_pdf_processing(
            uploaded_file.name,
            file_content,
            total_pages,
            openai_file_ids
        )
        
        st.session_state.current_pdf_id = pdf_id
        
        # Log analytics
        db.log_event('pdf_upload', {
            'file_name': uploaded_file.name,
            'file_size': len(file_content),
            'total_pages': total_pages,
            'chunks_count': len(file_chunks)
        }, st.session_state.session_id)
        
        st.success(f"✅ Processed {len(file_chunks)} chunks from new PDF")
        
    return file_chunks

def save_pdf_qa_to_db(question: str, answer: str):
    """Save PDF Q&A to database and session state"""
    if hasattr(st.session_state, 'current_pdf_id'):
        qa_id = db.save_pdf_qa(
            st.session_state.current_pdf_id,
            question,
            answer,
            query_tokens=len(question.split()) * 1.3,  # Rough estimate
            response_tokens=len(answer.split()) * 1.3   # Rough estimate
        )
        
        # Also add to session state for immediate display
        if "pdf_responses" not in st.session_state:
            st.session_state["pdf_responses"] = []
        
        qa_pair = {
            "question": question,
            "answer": answer,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        st.session_state["pdf_responses"].append(qa_pair)
        
        return qa_id
    return None

def restore_complete_market_analysis(market_name: str):
    """Restore a complete market analysis (all types) to session state"""
    try:
        # Get all cached analysis types for this market
        global_cached = db.get_cached_result(market_name, 'global')
        vertical_cached = db.get_cached_result(market_name, 'vertical')
        horizontal_cached = db.get_cached_result(market_name, 'horizontal')
        
        # Restore to session state
        if global_cached:
            st.session_state["global_md"] = global_cached['data']
        
        if vertical_cached or horizontal_cached:
            raw_markdown = ""
            if vertical_cached:
                raw_markdown += vertical_cached['data']
            if horizontal_cached:
                raw_markdown += f"\n\n{horizontal_cached['data']}"
            
            st.session_state["raw_markdown"] = raw_markdown
            
            # Process tables
            vertical_table_md, horizontal_table_md = split_tables(raw_markdown)
            st.session_state["vertical_df"] = markdown_table_to_dataframe(vertical_table_md)
            st.session_state["horizontal_df"] = markdown_table_to_dataframe(horizontal_table_md)
        
        st.session_state["market_analyzed"] = market_name
        return True
        
    except Exception as e:
        st.error(f"Failed to restore market analysis: {e}")
        return False

def restore_pdf_session_complete(pdf_id: int):
    """Restore a complete PDF session to session state"""
    try:
        session_data = db.restore_pdf_session(pdf_id)
        if not session_data:
            return False
        
        pdf_info = session_data['pdf_info']
        qa_history = session_data['qa_history']
        
        # Reconstruct file chunks
        openai_file_ids = json.loads(pdf_info['openai_file_ids'])
        file_chunks = []
        pages_per_chunk = pdf_info['total_pages'] // pdf_info['chunks_count'] if pdf_info['chunks_count'] > 0 else 50
        
        for i, file_id in enumerate(openai_file_ids):
            start_page = i * pages_per_chunk + 1
            end_page = min((i + 1) * pages_per_chunk, pdf_info['total_pages'])
            file_chunks.append({
                'file_id': file_id,
                'start': start_page,
                'end': end_page
            })
        
        # Restore to session state
        st.session_state["pdf_file_id_chunks"] = file_chunks
        st.session_state["uploaded_pdf_name"] = pdf_info['file_name']
        st.session_state["current_pdf_id"] = pdf_id
        
        # Restore Q&A history
        st.session_state["pdf_responses"] = []
        for qa in reversed(qa_history):
            st.session_state["pdf_responses"].append({
                "question": qa['question'],
                "answer": qa['answer'],
                "timestamp": qa['created_at']
            })
        
        return True
        
    except Exception as e:
        st.error(f"Failed to restore PDF session: {e}")
        return False

# === STREAMLIT TABS ===
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Market Sub-Segment Explorer",
    "📂 File-PDF Market Insight", 
    "🤝 M&A Explorer",
    "📄 Compare Multiple PDFs",
    "📚 History Browser",
    "🛠️ Admin Dashboard"
])

# === TAB 1: Market Analysis ===
with tab1:
    st.title("📊 Market Sub-Segment Explorer")

    # Market input form
    with st.form("market_input_form"):
        market = st.text_input("Enter a market (e.g. AI, Plastics, EV Batteries):", key="market_input")
        submitted = st.form_submit_button("Analyze Market")

    if submitted and market:
        # Get cached analyses with cache status
        with st.spinner("Analyzing market data..."):
            global_md, global_cached = get_cached_market_analysis(market, 'global')
            verticals_md, vertical_cached = get_cached_market_analysis(market, 'vertical')
            horizontals_md, horizontal_cached = get_cached_market_analysis(market, 'horizontal')
        
        # Show cache status
        cache_status = []
        if global_cached: cache_status.append("Global")
        if vertical_cached: cache_status.append("Vertical")  
        if horizontal_cached: cache_status.append("Horizontal")
        
        if cache_status:
            st.info(f"📋 Using cached data for: {', '.join(cache_status)}")
        
        # Store in session state
        st.session_state["global_md"] = global_md
        st.session_state["market_analyzed"] = market
        st.session_state["raw_markdown"] = f"{verticals_md}\n\n{horizontals_md}"
        
        # Process tables
        vertical_table_md, horizontal_table_md = split_tables(st.session_state["raw_markdown"])
        st.session_state["vertical_df"] = markdown_table_to_dataframe(vertical_table_md)
        st.session_state["horizontal_df"] = markdown_table_to_dataframe(horizontal_table_md)

    # Display results
    if st.session_state.get("global_md"):
        st.markdown("## Market Overview")
        if st.session_state.get("market_analyzed"):
            st.markdown(f"**Market:** {st.session_state['market_analyzed']}")
        st.markdown(st.session_state["global_md"])

    if st.session_state.get("vertical_df") is not None and not st.session_state["vertical_df"].empty:
        st.markdown("## 🏗️ Vertical Sub-markets")
        st.dataframe(st.session_state["vertical_df"], use_container_width=True)

    if st.session_state.get("horizontal_df") is not None and not st.session_state["horizontal_df"].empty:
        st.markdown("## 🔗 Horizontal Sub-markets")
        st.dataframe(st.session_state["horizontal_df"], use_container_width=True)

    # Download button
    if st.session_state.get("raw_markdown"):
        market_name = st.session_state.get("market_analyzed", "market")
        st.download_button(
            label="📥 Download Markdown",
            data=st.session_state["raw_markdown"],
            file_name=f"{market_name.replace(' ', '_')}_submarkets.md",
            mime="text/markdown"
        )

    # CACHED SIDEBAR WITH RADIO BUTTONS
    with st.sidebar:
        # Cache popular markets to prevent constant reloading
        @st.cache_data(ttl=300)  # Cache for 5 minutes
        def get_cached_popular_markets():
            return db.get_popular_markets(days=7, limit=5)
        
        # Only show popular markets section if enabled
        if st.session_state.get("show_popular_markets", True):
            st.markdown("## 🔥 Popular Markets")
            try:
                popular_markets = get_cached_popular_markets()
                if popular_markets:
                    for market_data in popular_markets:
                        if st.button(f"🎯 {market_data['market_name']} ({market_data['query_count']})", 
                                   key=f"popular_{market_data['market_name']}"):
                            st.session_state.suggested_market = market_data['market_name']
                            st.rerun()
                else:
                    st.caption("No popular markets yet")
            except Exception:
                st.caption("Popular markets unavailable")
        
        # Add toggle to hide/show popular markets
        if st.checkbox("Hide popular markets", key="hide_popular"):
            st.session_state.show_popular_markets = False
        else:
            st.session_state.show_popular_markets = True
        
        st.markdown("## 🔍 Sub-market Drilldown")

        vertical_df = st.session_state.get("vertical_df")
        horizontal_df = st.session_state.get("horizontal_df")

        if vertical_df is not None and not vertical_df.empty:
            vertical_col = vertical_df.columns[0]
            clicked_vertical = st.radio(
                "Select a vertical sub-market:",
                vertical_df[vertical_col].dropna().unique().tolist(),
                key="clicked_vertical"
            )

            if clicked_vertical:
                st.markdown(f"### 📈 Metrics for: {clicked_vertical}")
                st.markdown(get_detailed_metrics(clicked_vertical) or "⚠️ No metrics found.")
                st.markdown(f"### 🏢 Top Companies: {clicked_vertical}")
                st.markdown(get_top_companies(clicked_vertical) or "⚠️ No company data found.")

        if horizontal_df is not None and not horizontal_df.empty:
            horiz_col = horizontal_df.columns[0]
            clicked_horizontal = st.radio(
                "Select a horizontal sub-market:",
                horizontal_df[horiz_col].dropna().unique().tolist(),
                key="clicked_horizontal"
            )

            if clicked_horizontal:
                st.markdown(f"### 📈 Metrics for: {clicked_horizontal}")
                st.markdown(get_detailed_metrics(clicked_horizontal) or "⚠️ No metrics found.")
                st.markdown(f"### 🏢 Top Companies: {clicked_horizontal}")
                st.markdown(get_top_companies(clicked_horizontal) or "⚠️ No company data found.")

    # Manual query expander
    with st.expander("🔎 Manual Query: Explore Any Market"):
        custom_query = st.text_input("Enter any market (e.g. US plastic recycling, AI chips, etc.):", key="manual_input")
        if st.button("Run Custom Exploration"):
            with st.spinner("Fetching metrics and companies..."):
                st.markdown("### 📈 Custom Metrics")
                st.markdown(get_detailed_metrics(custom_query) or "⚠️ No metrics found.")
                st.markdown("### 🏢 Custom Companies")
                st.markdown(get_top_companies(custom_query) or "⚠️ No company data found.")

    # Clear button for Tab 1
    if st.button("🗑️ Clear Tab 1 Data"):
        keys_to_clear = ["global_md", "raw_markdown", "vertical_df", "horizontal_df", "market_analyzed"]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

# === TAB 2: PDF Processing ===
with tab2:
    st.title("📂 PDF-Based Market Insights")
    
    # Display current PDF info if exists
    if st.session_state.get("uploaded_pdf_name"):
        st.info(f"📄 Currently loaded: {st.session_state['uploaded_pdf_name']}")
        st.info(f"📊 Chunks available: {len(st.session_state.get('pdf_file_id_chunks', []))}")
    
    # File upload with deduplication
    with st.form("pdf_upload_form"):
        uploaded_file = st.file_uploader("Upload a PDF file", type=["pdf"])
        upload_pdf = st.form_submit_button("Upload and Process")

    if upload_pdf and uploaded_file:
        file_chunks = process_pdf_with_deduplication(uploaded_file)
        st.session_state["pdf_file_id_chunks"] = file_chunks
        st.session_state["uploaded_pdf_name"] = uploaded_file.name
        
        # Load existing Q&A history from database if PDF was processed before
        if hasattr(st.session_state, 'current_pdf_id'):
            qa_history = db.get_pdf_qa_history(st.session_state.current_pdf_id)
            if qa_history:
                # Convert DB format to session state format
                st.session_state["pdf_responses"] = []
                for qa in reversed(qa_history):  # Show newest first
                    st.session_state["pdf_responses"].append({
                        "question": qa['question'],
                        "answer": qa['answer'],
                        "timestamp": qa['created_at']
                    })

    # Query form with database storage
    if st.session_state.get("pdf_file_id_chunks"):
        with st.form("query_pdf_chunks"):
            query = st.text_input("Enter a question to ask your uploaded PDF:")
            submit_query = st.form_submit_button("Ask")

        if submit_query and query:
            with st.spinner("🤖 Querying your uploaded PDF..."):
                try:
                    response = query_chunks(query, st.session_state["pdf_file_id_chunks"])
                    
                    # Save to database AND session state
                    save_pdf_qa_to_db(query, response)
                    
                    # Log analytics
                    db.log_event('pdf_query', {
                        'question_length': len(query),
                        'answer_length': len(response) if response else 0,
                        'pdf_name': st.session_state.get("uploaded_pdf_name")
                    }, st.session_state.session_id)
                    
                    st.markdown("### 📑 Latest Response:")
                    st.markdown(response or "⚠️ No answer returned.")
                    
                except Exception as e:
                    st.error(f"❌ Error during query: {e}")

        # Display Q&A history from session state (which includes DB data)
        if st.session_state.get("pdf_responses"):
            st.markdown("## 📚 Previous Questions & Answers")
            for i, qa in enumerate(st.session_state["pdf_responses"]):
                with st.expander(f"Q{i+1}: {qa['question'][:50]}..."):
                    st.markdown(f"**Question:** {qa['question']}")
                    st.markdown(f"**Answer:**")
                    st.markdown(qa['answer'] or "⚠️ No answer returned.")
                    st.caption(f"Asked on {qa['timestamp']}")
                    st.download_button(
                        f"📥 Download Response {i+1}", 
                        data=qa['answer'], 
                        file_name=f"pdf_response_{i+1}.md", 
                        mime="text/markdown",
                        key=f"download_pdf_{i}"
                    )

    # Add clear button for Tab 2
    if st.button("🗑️ Clear PDF Data"):
        keys_to_clear = ["pdf_file_id_chunks", "pdf_responses", "uploaded_pdf_name", "current_pdf_id"]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

# === TAB 3: M&A Explorer ===
with tab3:
    st.title("🤝 Mergers & Acquisitions Explorer")
    
    # Display previous results if they exist
    if st.session_state.get("ma_results"):
        st.markdown(f"## Previous M&A Analysis: {st.session_state.get('ma_market_searched', 'Unknown')}")
        st.markdown(st.session_state["ma_results"])

    st.markdown("Explore recent M&A activity related to the market.")
    
    # Main form
    ma_market = st.text_input("Enter the market or industry:", 
                             value=st.session_state.get('suggested_ma_market', ''))
    col1, col2 = st.columns(2)
    with col1:
        timeframe_option = st.selectbox("Select timeframe:", 
                                       ["Last 3 years", "Last 5 years", "Custom Range"])
    with col2:
        custom_range = st.text_input("If custom, enter range (e.g., 2018–2020):")

    if st.button("🔍 Search M&A Activity"):
        if ma_market:
            timeframe_str = custom_range if timeframe_option == "Custom Range" and custom_range else timeframe_option.lower()
            
            with st.spinner("Searching for M&A activity..."):
                result = get_mergers_table(ma_market, timeframe_str)
                
                # Save to database
                deals_count = result.count('|') // 7 if result else 0
                db.save_ma_search(ma_market, timeframe_str, result, deals_count)
                
                # Log analytics
                db.log_event('ma_search', {
                    'market_name': ma_market,
                    'timeframe': timeframe_str,
                    'deals_found': deals_count
                }, st.session_state.session_id)
                
                # Update session state
                st.session_state["ma_results"] = result
                st.session_state["ma_market_searched"] = ma_market
                
                st.markdown("## 🔍 M&A Analysis Results")
                st.markdown(result or "⚠️ No data found.")

    # Cached sidebar for recent searches
    with st.sidebar:
        @st.cache_data(ttl=300)  # Cache for 5 minutes
        def get_cached_recent_ma():
            return db.get_recent_ma_searches(limit=5)
        
        if st.session_state.get("show_recent_ma", True):
            st.subheader("🕒 Recent M&A Searches")
            try:
                recent_searches = get_cached_recent_ma()
                if recent_searches:
                    for search in recent_searches:
                        if st.button(f"🔍 {search['market_name'][:15]}...", 
                                   key=f"recent_ma_{search['market_name']}_{search['created_at'][:10]}"):
                            st.session_state.suggested_ma_market = search['market_name']
                            st.rerun()
                else:
                    st.caption("No recent searches")
            except Exception:
                st.caption("Recent searches unavailable")
        
        # Toggle for recent searches
        if st.checkbox("Hide recent searches", key="hide_recent_ma"):
            st.session_state.show_recent_ma = False
        else:
            st.session_state.show_recent_ma = True

    # Add clear button for Tab 3
    if st.button("🗑️ Clear M&A Data"):
        keys_to_clear = ["ma_results", "ma_market_searched", "suggested_ma_market"]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

# === TAB 4: PDF Comparison ===
with tab4:
    st.title("📄 Compare Multiple PDFs")
    
    # Display previous comparison results
    if st.session_state.get("comparison_results"):
        st.markdown("## 📊 Previous Comparison Results")
        st.markdown(f"**Files compared:** {', '.join(st.session_state.get('compared_files', []))}")
        
        comparison_results = st.session_state["comparison_results"]
        web_insights_results = st.session_state.get("web_insights_results")
        
        total_columns = len(comparison_results) + (1 if web_insights_results else 0)
        columns = st.columns(total_columns)

        for idx, (filename, result) in enumerate(comparison_results.items()):
            with columns[idx]:
                st.markdown(f"### 📄 {filename}")
                st.markdown(result)

        if web_insights_results:
            with columns[-1]:
                st.markdown("### 🌐 Web Search Insights")
                st.markdown(web_insights_results or "⚠️ No web results.")

    # Move checkbox and web prompt outside the form for live interactivity
    enable_web_search = st.checkbox("Enable Web Search")
    prompt_web = ""
    if enable_web_search:
        prompt_web = st.text_input("Enter a web search prompt (optional):")

    with st.form("compare_form"):
        pdf_files = st.file_uploader("Upload up to 5 PDF files", type=["pdf"], accept_multiple_files=True)
        prompt = st.text_input("Enter a comparison prompt (e.g. 'List all CAGR values' or 'Compare market sizes'):")
        run_comparison = st.form_submit_button("Compare PDFs")

    if run_comparison:
        if not pdf_files or len(pdf_files) < 2:
            st.warning("Please upload at least two PDFs to compare.")
        elif not prompt:
            st.warning("Please enter a prompt for comparison.")
        else:
            with st.spinner("Analyzing and comparing PDFs..."):
                try:
                    comparison_dict = compare_uploaded_pdfs(pdf_files, prompt)
                    st.session_state["comparison_results"] = comparison_dict
                    st.session_state["compared_files"] = [f.name for f in pdf_files]

                    # Perform web search if enabled
                    web_insights = None
                    if enable_web_search:
                        web_prompt = prompt_web if prompt_web else prompt
                        web_insights = search_web_insights(web_prompt)
                        st.session_state["web_insights_results"] = web_insights

                    # Layout results
                    st.markdown("## 📊 Comparison Result")
                    total_columns = len(comparison_dict) + (1 if web_insights else 0)
                    columns = st.columns(total_columns)

                    for idx, (filename, result) in enumerate(comparison_dict.items()):
                        with columns[idx]:
                            st.markdown(f"### 📄 {filename}")
                            st.markdown(result)

                    if web_insights:
                        with columns[-1]:
                            st.markdown("### 🌐 Web Search Insights")
                            st.markdown(web_insights or "⚠️ No web results.")

                except Exception as e:
                    st.error(f"❌ Error: {e}")

    # Add clear button for Tab 4
    if st.button("🗑️ Clear Comparison Data"):
        keys_to_clear = ["comparison_results", "web_insights_results", "compared_files"]
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

# === TAB 5: Enhanced History Browser ===
with tab5:
    st.title("📚 History Browser")
    st.markdown("Browse, restore, and manage your previous analyses and PDF sessions.")
    
    # Search/Filter with enhanced controls
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        search_term = st.text_input("🔍 Search history:", placeholder="Enter market name or PDF filename...")
    with col2:
        history_type = st.selectbox("Filter by type:", ["All", "Market Analysis", "PDF Sessions", "M&A Searches"])
    with col3:
        show_delete_options = st.checkbox("🗑️ Show Delete Options", help="Enable delete buttons for data management")
    
    st.markdown("---")
    
    # Market Analysis History
    if history_type in ["All", "Market Analysis"]:
        st.subheader("📈 Market Analysis History")
        
        try:
            market_history = db.get_market_analysis_history(limit=20)
            
            if market_history:
                # Group by market name for better organization
                markets_dict = {}
                for item in market_history:
                    market_name = item['market_name']
                    if search_term.lower() in market_name.lower() or not search_term:
                        if market_name not in markets_dict:
                            markets_dict[market_name] = []
                        markets_dict[market_name].append(item)
                
                for market_name, analyses in markets_dict.items():
                    with st.expander(f"🎯 **{market_name}** ({len(analyses)} analyses)", expanded=False):
                        # Action buttons row
                        col1, col2, col3 = st.columns([2, 1, 1])
                        
                        with col1:
                            # Show available analysis types
                            analysis_types = [a['query_type'] for a in analyses]
                            st.write(f"**Available:** {', '.join(analysis_types)}")
                            
                            # Show most recent date
                            latest_date = max([a['created_at'] for a in analyses])
                            st.caption(f"Last updated: {latest_date}")
                        
                        with col2:
                            if st.button(f"🔄 Restore All", key=f"restore_market_{market_name}"):
                                if restore_complete_market_analysis(market_name):
                                    st.success(f"✅ Restored complete analysis for {market_name}!")
                                    st.info("👈 Check Tab 1 to see the results")
                        
                        with col3:
                            if show_delete_options:
                                if st.button(f"🗑️ Delete", key=f"delete_market_{market_name}", type="secondary"):
                                    # Confirmation check
                                    confirm_key = f"confirm_delete_market_{market_name}"
                                    if st.session_state.get(confirm_key, False):
                                        # Delete all analyses for this market
                                        with sqlite3.connect(db.db_path) as conn:
                                            cursor = conn.execute("""
                                                DELETE FROM market_cache WHERE market_name = ?
                                            """, (market_name,))
                                            deleted_count = cursor.rowcount
                                        
                                        st.success(f"✅ Deleted {deleted_count} analyses for {market_name}")
                                        st.session_state[confirm_key] = False
                                        st.rerun()
                                    else:
                                        st.session_state[confirm_key] = True
                                        st.warning(f"⚠️ Click again to confirm deletion of {market_name}")
                        
                        # Show individual analysis types with delete options
                        if show_delete_options:
                            st.markdown("**Individual Analysis Types:**")
                            for analysis in analyses:
                                col_a, col_b, col_c = st.columns([2, 1, 1])
                                with col_a:
                                    st.write(f"• **{analysis['query_type'].title()}** (accessed {analysis['access_count']} times)")
                                with col_b:
                                    st.caption(f"{analysis['created_at'][:16]}")
                                with col_c:
                                    delete_key = f"delete_{market_name}_{analysis['query_type']}"
                                    if st.button("🗑️", key=delete_key, help=f"Delete {analysis['query_type']} analysis"):
                                        # Delete specific analysis type
                                        with sqlite3.connect(db.db_path) as conn:
                                            conn.execute("""
                                                DELETE FROM market_cache 
                                                WHERE market_name = ? AND query_type = ?
                                            """, (market_name, analysis['query_type']))
                                        
                                        st.success(f"✅ Deleted {analysis['query_type']} analysis for {market_name}")
                                        st.rerun()
                        else:
                            # Show individual analysis types without delete
                            cols = st.columns(len(analyses))
                            for i, analysis in enumerate(analyses):
                                with cols[i]:
                                    st.write(f"**{analysis['query_type'].title()}**")
                                    st.caption(f"Accessed {analysis['access_count']} times")
            else:
                st.info("No market analysis history found. Analyze some markets first!")
        except Exception as e:
            st.error(f"Error loading market history: {e}")
    
    # PDF Sessions History  
    if history_type in ["All", "PDF Sessions"]:
        st.subheader("📄 PDF Session History")
        
        try:
            pdf_sessions = db.get_pdf_sessions_summary(limit=15)
            
            if pdf_sessions:
                for pdf in pdf_sessions:
                    if not search_term or search_term.lower() in pdf['file_name'].lower():
                        with st.expander(f"📄 **{pdf['file_name']}** ({pdf['qa_count']} Q&As)", expanded=False):
                            # Main info and action buttons
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.write(f"**Pages:** {pdf['total_pages']} | **Chunks:** {pdf['chunks_count']}")
                                st.write(f"**Processed:** {pdf['processed_at'][:16]}")
                                if pdf['last_question']:
                                    st.write(f"**Last Q&A:** {pdf['last_question'][:16]}")
                            
                            with col2:
                                if st.button("🔄 Restore Session", key=f"restore_pdf_{pdf['id']}"):
                                    if restore_pdf_session_complete(pdf['id']):
                                        st.success(f"✅ Restored PDF session!")
                                        st.info("👈 Check Tab 2 to continue with this PDF")
                            
                            with col3:
                                if show_delete_options:
                                    if st.button("🗑️ Delete PDF", key=f"delete_pdf_{pdf['id']}", type="secondary"):
                                        # Confirmation check
                                        confirm_key = f"confirm_delete_pdf_{pdf['id']}"
                                        if st.session_state.get(confirm_key, False):
                                            # Delete PDF and all associated Q&As
                                            with sqlite3.connect(db.db_path) as conn:
                                                # Delete Q&As first (foreign key constraint)
                                                qa_cursor = conn.execute("""
                                                    DELETE FROM pdf_qa WHERE pdf_history_id = ?
                                                """, (pdf['id'],))
                                                qa_deleted = qa_cursor.rowcount
                                                
                                                # Delete PDF record
                                                conn.execute("""
                                                    DELETE FROM pdf_history WHERE id = ?
                                                """, (pdf['id'],))
                                            
                                            st.success(f"✅ Deleted PDF '{pdf['file_name']}' and {qa_deleted} Q&As")
                                            st.session_state[confirm_key] = False
                                            st.rerun()
                                        else:
                                            st.session_state[confirm_key] = True
                                            st.warning(f"⚠️ Click again to confirm deletion of '{pdf['file_name']}'")
                            
                            # Show Q&A preview with individual delete options
                            if pdf['qa_count'] > 0:
                                qa_preview = db.get_pdf_qa_history(pdf['id'])
                                if qa_preview:
                                    st.markdown("**Recent Questions:**")
                                    
                                    if show_delete_options:
                                        # Show detailed Q&A list with delete options
                                        for i, qa in enumerate(qa_preview[:5]):  # Show latest 5
                                            col_a, col_b = st.columns([4, 1])
                                            with col_a:
                                                st.write(f"• {qa['question'][:80]}...")
                                                st.caption(f"Asked on {qa['created_at'][:16]}")
                                            with col_b:
                                                if st.button("🗑️", key=f"delete_qa_{pdf['id']}_{i}", 
                                                            help="Delete this Q&A"):
                                                    # Delete specific Q&A
                                                    with sqlite3.connect(db.db_path) as conn:
                                                        conn.execute("""
                                                            DELETE FROM pdf_qa 
                                                            WHERE pdf_history_id = ? AND question = ?
                                                        """, (pdf['id'], qa['question']))
                                                    
                                                    st.success("✅ Deleted Q&A")
                                                    st.rerun()
                                        
                                        if len(qa_preview) > 5:
                                            st.caption(f"... and {len(qa_preview) - 5} more Q&As")
                                    else:
                                        # Show simple preview without delete options
                                        for qa in qa_preview[:3]:  # Show latest 3
                                            st.write(f"• {qa['question'][:60]}...")
            else:
                st.info("No PDF sessions found. Upload some PDFs first!")
        except Exception as e:
            st.error(f"Error loading PDF history: {e}")
    
    # M&A History
    if history_type in ["All", "M&A Searches"]:
        st.subheader("🤝 M&A Search History")
        
        try:
            recent_ma = db.get_recent_ma_searches(limit=15)
            
            if recent_ma:
                for i, search in enumerate(recent_ma):
                    if not search_term or search_term.lower() in search['market_name'].lower():
                        with st.expander(f"🔍 **{search['market_name']}** ({search['timeframe']})", expanded=False):
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.write(f"**Timeframe:** {search['timeframe']}")
                                st.write(f"**Deals found:** {search['deals_count']}")
                                st.write(f"**Searched:** {search['created_at'][:16]}")
                            
                            with col2:
                                if st.button("🔄 Restore Search", key=f"restore_ma_{search['market_name']}_{i}"):
                                    st.session_state["ma_results"] = search.get('result_data', '')
                                    st.session_state["ma_market_searched"] = search['market_name']
                                    st.success(f"✅ Restored M&A search!")
                                    st.info("👈 Check Tab 3 to see the results")
                            
                            with col3:
                                if show_delete_options:
                                    if st.button("🗑️ Delete", key=f"delete_ma_{search['market_name']}_{i}", type="secondary"):
                                        # Delete specific M&A search
                                        with sqlite3.connect(db.db_path) as conn:
                                            conn.execute("""
                                                DELETE FROM ma_searches 
                                                WHERE market_name = ? AND created_at = ?
                                            """, (search['market_name'], search['created_at']))
                                        
                                        st.success(f"✅ Deleted M&A search for {search['market_name']}")
                                        st.rerun()
            else:
                st.info("No M&A search history found.")
        except Exception as e:
            st.error(f"Error loading M&A history: {e}")
    
    # Enhanced Quick Actions
    st.markdown("---")
    st.subheader("🚀 Quick Actions")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        if st.button("🔄 Refresh History"):
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear Expired"):
            db.cleanup_expired_cache()
            st.success("Expired cache cleared!")
    
    with col3:
        # Show total items with delete all option
        stats = db.get_database_stats()
        total_items = stats['market_cache_count'] + stats['pdf_history_count'] + stats['ma_searches_count']
        st.metric("Total Items", total_items)
    
    with col4:
        # Bulk delete options
        if show_delete_options:
            delete_option = st.selectbox("Bulk Delete:", 
                                       ["Select...", "All Markets", "All PDFs", "All M&A", "Everything"],
                                       key="bulk_delete_option")
            
            if delete_option != "Select...":
                if st.button("⚠️ Execute", type="secondary", key="bulk_delete_execute"):
                    confirm_key = f"confirm_bulk_{delete_option}"
                    
                    if st.session_state.get(confirm_key, False):
                        with sqlite3.connect(db.db_path) as conn:
                            if delete_option == "All Markets":
                                cursor = conn.execute("DELETE FROM market_cache")
                                deleted = cursor.rowcount
                                st.success(f"✅ Deleted {deleted} market analyses")
                            elif delete_option == "All PDFs":
                                conn.execute("DELETE FROM pdf_qa")
                                cursor = conn.execute("DELETE FROM pdf_history")
                                deleted = cursor.rowcount
                                st.success(f"✅ Deleted {deleted} PDF sessions")
                            elif delete_option == "All M&A":
                                cursor = conn.execute("DELETE FROM ma_searches")
                                deleted = cursor.rowcount
                                st.success(f"✅ Deleted {deleted} M&A searches")
                            elif delete_option == "Everything":
                                conn.executescript("""
                                    DELETE FROM market_cache;
                                    DELETE FROM pdf_qa;
                                    DELETE FROM pdf_history;
                                    DELETE FROM ma_searches;
                                """)
                                st.success("✅ Deleted all history data")
                        
                        st.session_state[confirm_key] = False
                        st.rerun()
                    else:
                        st.session_state[confirm_key] = True
                        st.error(f"⚠️ **DANGER**: This will delete {delete_option}! Click Execute again to confirm.")
        else:
            st.info("Enable delete options above")
    
    with col5:
        # Enhanced export with filtering
        if st.button("📦 Export Filtered", help="Export currently filtered data"):
            try:
                import zipfile
                import io
                
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    
                    # Export based on current filter
                    if history_type in ["All", "Market Analysis"]:
                        # Export market analyses
                        with sqlite3.connect(db.db_path) as conn:
                            cursor = conn.execute("""
                                SELECT market_name, query_type, result_data, created_at
                                FROM market_cache
                                WHERE (expires_at > datetime('now') OR expires_at IS NULL)
                                AND (? = '' OR market_name LIKE ?)
                            """, (search_term, f'%{search_term}%'))
                            
                            for market_name, query_type, result_data, created_at in cursor.fetchall():
                                safe_name = market_name.replace('/', '_').replace(' ', '_')
                                filename = f"market_{safe_name}_{query_type}_{created_at[:10]}.md"
                                zip_file.writestr(filename, result_data)
                    
                    # Export PDF Q&As (if in filter)
                    if history_type in ["All", "PDF Sessions"]:
                        with sqlite3.connect(db.db_path) as conn:
                            cursor = conn.execute("""
                                SELECT p.file_name, q.question, q.answer, q.created_at
                                FROM pdf_history p
                                JOIN pdf_qa q ON p.id = q.pdf_history_id
                                WHERE (? = '' OR p.file_name LIKE ?)
                                ORDER BY p.file_name, q.created_at
                            """, (search_term, f'%{search_term}%'))
                            
                            current_pdf = ""
                            pdf_content = ""
                            
                            for file_name, question, answer, created_at in cursor.fetchall():
                                if file_name != current_pdf:
                                    if pdf_content:
                                        safe_name = current_pdf.replace('/', '_').replace(' ', '_')
                                        zip_file.writestr(f"pdf_{safe_name}_qa.md", pdf_content)
                                    current_pdf = file_name
                                    pdf_content = f"# Q&A for {file_name}\n\n"
                                
                                pdf_content += f"## Q: {question}\n**Asked:** {created_at}\n\n{answer}\n\n---\n\n"
                            
                            if pdf_content:
                                safe_name = current_pdf.replace('/', '_').replace(' ', '_')
                                zip_file.writestr(f"pdf_{safe_name}_qa.md", pdf_content)
                
                zip_buffer.seek(0)
                
                filter_suffix = f"_{history_type.lower().replace(' ', '_')}" if history_type != "All" else ""
                search_suffix = f"_{search_term}" if search_term else ""
                
                st.download_button(
                    label="📥 Download Filtered Export",
                    data=zip_buffer.getvalue(),
                    file_name=f"history_export{filter_suffix}{search_suffix}_{datetime.now().strftime('%Y%m%d')}.zip",
                    mime="application/zip",
                    key="export_filtered_zip"
                )
                
            except Exception as e:
                st.error(f"Export failed: {e}")

# === TAB 6: Enhanced Admin Dashboard ===
with tab6:
    st.title("🛠️ Database Administration")
    st.markdown("---")
    
    # === OVERVIEW SECTION ===
    st.markdown("### 📊 Database Overview")
    stats = db.get_database_stats()
    
    # Enhanced metrics with colors and icons
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📋 Cached Results", stats['market_cache_count'], 
                 delta=None, delta_color="normal")
        st.metric("📊 Usage Events", stats['usage_analytics_count'])
    with col2:
        st.metric("📄 Processed PDFs", stats['pdf_history_count'])
        st.metric("💬 PDF Q&As", stats['pdf_qa_count'])
    with col3:
        st.metric("🤝 M&A Searches", stats['ma_searches_count'])
        db_size_color = "normal" if stats['db_size_mb'] < 10 else "inverse"
        st.metric("💾 DB Size (MB)", f"{stats['db_size_mb']:.2f}")
    with col4:
        # Quick actions
        if st.button("🧹 Clean Expired Cache", type="secondary"):
            db.cleanup_expired_cache()
            st.success("✅ Expired cache cleaned!")
        
        if st.button("🔄 Refresh Stats", type="secondary"):
            st.rerun()
        
        if st.button("📊 View Analytics", type="primary"):
            st.session_state.show_analytics = True

    st.markdown("---")

    # === MANAGEMENT SECTION ===
    st.markdown("### 🗂️ Data Management")
    
    management_tab1, management_tab2, management_tab3 = st.tabs([
        "🗑️ Delete Data", 
        "📈 Analytics", 
        "⚙️ Maintenance"
    ])
    
    # DELETE DATA TAB
    with management_tab1:
        st.markdown("#### 🗑️ Selective Data Deletion")
        st.warning("⚠️ **Warning**: Deleted data cannot be recovered!")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 📋 Market Analysis Cache")
            
            # Get all cached markets for selection
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.execute("""
                    SELECT DISTINCT market_name, COUNT(*) as count
                    FROM market_cache 
                    GROUP BY market_name 
                    ORDER BY market_name
                """)
                cached_markets = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
            
            if cached_markets:
                # Multi-select for markets to delete
                markets_to_delete = st.multiselect(
                    "Select markets to delete:",
                    options=[m["name"] for m in cached_markets],
                    format_func=lambda x: f"{x} ({next(m['count'] for m in cached_markets if m['name'] == x)} entries)"
                )
                
                if markets_to_delete:
                    if st.button("🗑️ Delete Selected Markets", type="secondary"):
                        with sqlite3.connect(db.db_path) as conn:
                            for market in markets_to_delete:
                                conn.execute("DELETE FROM market_cache WHERE market_name = ?", (market,))
                        st.success(f"✅ Deleted {len(markets_to_delete)} markets from cache")
                        st.rerun()
            else:
                st.info("No cached markets found")
            
            # Delete all cache option
            if st.button("🗑️ Clear ALL Market Cache", type="secondary"):
                if st.session_state.get("confirm_clear_cache", False):
                    with sqlite3.connect(db.db_path) as conn:
                        conn.execute("DELETE FROM market_cache")
                    st.success("✅ All market cache cleared!")
                    st.session_state.confirm_clear_cache = False
                    st.rerun()
                else:
                    st.session_state.confirm_clear_cache = True
                    st.error("⚠️ Click again to confirm deletion of ALL cached markets")
        
        with col2:
            st.markdown("##### 📄 PDF Data")
            
            # Get PDF sessions for deletion
            pdf_sessions = db.get_pdf_sessions_summary(limit=20)
            
            if pdf_sessions:
                # Select PDFs to delete
                pdfs_to_delete = st.multiselect(
                    "Select PDF sessions to delete:",
                    options=[(p["id"], p["file_name"]) for p in pdf_sessions],
                    format_func=lambda x: f"{x[1]} ({next(p['qa_count'] for p in pdf_sessions if p['id'] == x[0])} Q&As)"
                )
                
                if pdfs_to_delete:
                    if st.button("🗑️ Delete Selected PDFs", type="secondary"):
                        with sqlite3.connect(db.db_path) as conn:
                            for pdf_id, _ in pdfs_to_delete:
                                # Delete Q&As first (foreign key constraint)
                                conn.execute("DELETE FROM pdf_qa WHERE pdf_history_id = ?", (pdf_id,))
                                # Then delete PDF record
                                conn.execute("DELETE FROM pdf_history WHERE id = ?", (pdf_id,))
                        st.success(f"✅ Deleted {len(pdfs_to_delete)} PDF sessions")
                        st.rerun()
            else:
                st.info("No PDF sessions found")
            
            st.markdown("##### 🤝 M&A Searches")
            
            # M&A deletion options
            ma_searches = db.get_recent_ma_searches(limit=15)
            
            if ma_searches:
                # Date range deletion
                col_a, col_b = st.columns(2)
                with col_a:
                    days_old = st.selectbox("Delete M&A searches older than:", 
                                          [7, 30, 90, 180, 365], 
                                          format_func=lambda x: f"{x} days")
                with col_b:
                    if st.button("🗑️ Delete Old M&A Data", type="secondary"):
                        with sqlite3.connect(db.db_path) as conn:
                            cursor = conn.execute("""
                                DELETE FROM ma_searches 
                                WHERE created_at < datetime('now', '-{} days')
                            """.format(days_old))
                            deleted_count = cursor.rowcount
                        st.success(f"✅ Deleted {deleted_count} old M&A searches")
                        st.rerun()
            else:
                st.info("No M&A searches found")
    
    # ANALYTICS TAB
    with management_tab2:
        st.markdown("#### 📈 Advanced Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🔥 Popular Markets (Last 30 Days)")
            popular = db.get_popular_markets(days=30, limit=10)
            if popular:
                import pandas as pd
                df = pd.DataFrame(popular)
                
                # Enhanced dataframe display
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "market_name": st.column_config.TextColumn("Market", width="medium"),
                        "query_count": st.column_config.NumberColumn("Queries", format="%d"),
                        "last_queried": st.column_config.DatetimeColumn("Last Query")
                    }
                )
                
                # Enhanced chart
                st.bar_chart(df.set_index('market_name')['query_count'], height=300)
            else:
                st.info("📊 No market analysis data yet. Start analyzing some markets!")
        
        with col2:
            st.markdown("##### 📊 Usage Trends")
            
            # Usage over time
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.execute("""
                    SELECT DATE(created_at) as date, COUNT(*) as count
                    FROM usage_analytics 
                    WHERE created_at > datetime('now', '-30 days')
                    GROUP BY DATE(created_at)
                    ORDER BY date DESC
                    LIMIT 10
                """)
                usage_data = cursor.fetchall()
            
            if usage_data:
                usage_df = pd.DataFrame(usage_data, columns=['Date', 'Events'])
                st.line_chart(usage_df.set_index('Date'), height=200)
                
                # Event type breakdown
                with sqlite3.connect(db.db_path) as conn:
                    cursor = conn.execute("""
                        SELECT event_type, COUNT(*) as count
                        FROM usage_analytics 
                        WHERE created_at > datetime('now', '-7 days')
                        GROUP BY event_type
                        ORDER BY count DESC
                    """)
                    event_types = cursor.fetchall()
                
                if event_types:
                    st.markdown("**Activity This Week:**")
                    for event_type, count in event_types:
                        st.write(f"• {event_type.replace('_', ' ').title()}: {count} times")
            else:
                st.info("📈 No usage data available yet")
    
    # MAINTENANCE TAB
    with management_tab3:
        st.markdown("#### ⚙️ Database Maintenance")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### 🔧 Database Health")
            
            # Database health checks
            health_checks = []
            
            # Check database size
            if stats['db_size_mb'] > 50:
                health_checks.append(("⚠️", "Database size is getting large", "warning"))
            else:
                health_checks.append(("✅", "Database size is healthy", "success"))
            
            # Check for old cache entries
            with sqlite3.connect(db.db_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM market_cache 
                    WHERE created_at < datetime('now', '-7 days')
                """)
                old_cache_count = cursor.fetchone()[0]
            
            if old_cache_count > 50:
                health_checks.append(("⚠️", f"{old_cache_count} old cache entries", "warning"))
            else:
                health_checks.append(("✅", "Cache is clean", "success"))
            
            # Display health checks
            for icon, message, status in health_checks:
                if status == "warning":
                    st.warning(f"{icon} {message}")
                else:
                    st.success(f"{icon} {message}")
        
        with col2:
            st.markdown("##### 🛠️ Maintenance Actions")
            
            if st.button("🧹 Full Database Cleanup", type="primary"):
                with st.spinner("Performing full cleanup..."):
                    # Clean expired cache
                    db.cleanup_expired_cache()
                    
                    # Clean old usage analytics (keep last 90 days)
                    with sqlite3.connect(db.db_path) as conn:
                        cursor = conn.execute("""
                            DELETE FROM usage_analytics 
                            WHERE created_at < datetime('now', '-90 days')
                        """)
                        analytics_deleted = cursor.rowcount
                        
                        # Vacuum database to reclaim space
                        conn.execute("VACUUM")
                    
                    st.success(f"✅ Cleanup complete! Removed {analytics_deleted} old analytics entries")
                    st.rerun()
            
            if st.button("📁 Export Database", type="secondary"):
                # Create export data
                export_data = {
                    "timestamp": datetime.now().isoformat(),
                    "stats": stats,
                    "popular_markets": popular if 'popular' in locals() else [],
                }
                
                export_json = json.dumps(export_data, indent=2)
                st.download_button(
                    "📥 Download Export",
                    data=export_json,
                    file_name=f"database_export_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                    mime="application/json"
                )
            
            # Danger zone
            st.markdown("---")
            st.markdown("##### ⚠️ Danger Zone")
            
            if st.button("🗑️ RESET ALL DATA", type="secondary"):
                if st.session_state.get("confirm_reset", False):
                    with sqlite3.connect(db.db_path) as conn:
                        conn.executescript("""
                            DELETE FROM market_cache;
                            DELETE FROM pdf_qa;
                            DELETE FROM pdf_history;
                            DELETE FROM ma_searches;
                            DELETE FROM usage_analytics;
                            VACUUM;
                        """)
                    st.success("✅ All data has been reset!")
                    st.session_state.confirm_reset = False
                    st.rerun()
                else:
                    st.session_state.confirm_reset = True
                    st.error("⚠️ **DANGER**: This will delete ALL data! Click again to confirm.")

    st.markdown("---")
    
    # === QUICK STATS FOOTER ===
    st.markdown("### 📋 Quick Stats")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_queries = stats['market_cache_count'] + stats['pdf_qa_count'] + stats['ma_searches_count']
        st.metric("🔍 Total Queries", total_queries)
    
    with col2:
        # Calculate average queries per day (last 30 days)
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.execute("""
                SELECT COUNT(*) FROM usage_analytics 
                WHERE created_at > datetime('now', '-30 days')
            """)
            recent_activity = cursor.fetchone()[0]
        avg_daily = recent_activity / 30 if recent_activity > 0 else 0
        st.metric("📊 Avg Daily Activity", f"{avg_daily:.1f}")
    
    with col3:
        # Most active day
        with sqlite3.connect(db.db_path) as conn:
            cursor = conn.execute("""
                SELECT DATE(created_at) as date, COUNT(*) as count
                FROM usage_analytics 
                WHERE created_at > datetime('now', '-7 days')
                GROUP BY DATE(created_at)
                ORDER BY count DESC
                LIMIT 1
            """)
            most_active = cursor.fetchone()
        most_active_count = most_active[1] if most_active else 0
        st.metric("🏆 Peak Day Activity", most_active_count)
    
    with col4:
        # Storage efficiency
        avg_size_per_entry = (stats['db_size_mb'] * 1024) / max(total_queries, 1)
        st.metric("💾 KB per Entry", f"{avg_size_per_entry:.1f}")

# Add cleanup on app restart
if st.sidebar.button("🗑️ Clear All Session Data"):
    for key in list(st.session_state.keys()):
        if key != 'session_id':  # Keep session ID for analytics
            del st.session_state[key]
    st.rerun()