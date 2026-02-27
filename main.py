"""
main.py — Pathway Streaming Pipeline Entry Point
=================================================
Starts the real-time compliance RAG server. This is a long-running process
that listens to a Google Drive folder, parses incoming regulatory documents,
embeds them locally via CUDA, and serves a live vector search endpoint.

Usage:
    python src/main.py

The server will be available at http://127.0.0.1:8000 once running.
"""

import logging
import os

import pathway as pw
from dotenv import load_dotenv
from pathway.xpacks.llm.embedders import SentenceTransformerEmbedder
from pathway.xpacks.llm.parsers import DoclingParser
from pathway.xpacks.llm.vector_store import VectorStoreServer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

# Google Drive folder ID to monitor for regulatory documents.
# Share this folder with your service account email to grant access.
GDRIVE_FOLDER_ID: str = os.getenv("GDRIVE_FOLDER_ID", "1dvtPaHqjlQgQRbeqqi3yMfpOBFWVzDq3")

# Path to the Google Cloud service account credentials file.
# ⚠️  Never commit this file to version control.
SERVICE_ACCOUNT_FILE: str = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    "/home/ashwin/credentials.json",
)

# Sentence Transformer model for local embedding.
# "all-MiniLM-L6-v2" is a strong, lightweight choice for semantic search.
EMBEDDING_MODEL: str = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")

# Inference device — "cuda" for GPU acceleration, "cpu" as fallback.
EMBEDDING_DEVICE: str = os.getenv("EMBEDDING_DEVICE", "cuda")

# Host and port for the Pathway VectorStoreServer.
SERVER_HOST: str = "127.0.0.1"
SERVER_PORT: int = int(os.getenv("VECTOR_STORE_PORT", 8000))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_pipeline() -> VectorStoreServer:
    """
    Constructs and returns the full Pathway streaming RAG pipeline.

    Pipeline stages:
        1. Ingest  — Stream files from Google Drive (create / update / delete events)
        2. Parse   — Extract structured text chunks from complex PDFs via Docling
        3. Map     — Normalize chunk schema for the vector store
        4. Embed   — Generate dense vectors locally on CUDA (no external API calls)
        5. Serve   — Expose a live, always-consistent vector search endpoint
    """

    # ------------------------------------------------------------------
    # Stage 1: Streaming Ingestion
    # ------------------------------------------------------------------
    # pathway.io.gdrive registers a native file-event listener on the folder.
    # Any create / update / delete triggers an incremental re-index of only
    # the affected document — not the entire corpus.
    logger.info("Connecting to Google Drive folder: %s", GDRIVE_FOLDER_ID)

    data_source = pw.io.gdrive.read(
        object_id=GDRIVE_FOLDER_ID,
        service_user_credentials_file=SERVICE_ACCOUNT_FILE,
        with_metadata=True,   # Attaches filename, modified_time, etc. to each row
        mode="streaming",     # Enables live event-driven updates
    )

    # ------------------------------------------------------------------
    # Stage 2: Adaptive PDF Parsing
    # ------------------------------------------------------------------
    # DoclingParser handles complex financial layouts that standard parsers
    # struggle with: multi-column PDFs, nested tables, footnotes, etc.
    # chunk=True splits documents into semantically coherent passages,
    # which improves retrieval precision over naive fixed-size splitting.
    logger.info("Initializing Docling parser with chunking enabled.")

    parser = DoclingParser(chunk=True)

    chunks = (
        data_source
        .select(
            doc_chunks=parser(pw.this.data),  # Parse raw bytes → list of chunks
            metadata=pw.this._metadata,       # Preserve source document metadata
        )
        .flatten(pw.this.doc_chunks)          # One row per chunk (explode the list)
    )

    # ------------------------------------------------------------------
    # Stage 3: Schema Normalisation
    # ------------------------------------------------------------------
    # VectorStoreServer expects columns named `data` and `_metadata`.
    # We map from the parser output format to match that contract.
    documents = chunks.select(
        data=pw.this.doc_chunks[0],   # Chunk text content
        _metadata=pw.this.metadata,   # Source metadata (filename, timestamps, etc.)
    )

    # ------------------------------------------------------------------
    # Stage 4: Local GPU Embedding
    # ------------------------------------------------------------------
    # Vectors are computed entirely on-premise using SentenceTransformer.
    # This is a deliberate security design: regulatory documents may contain
    # MNPI (Material Non-Public Information) and must never leave the
    # secure environment via a third-party embedding API.
    logger.info(
        "Loading embedding model '%s' on device '%s'.",
        EMBEDDING_MODEL,
        EMBEDDING_DEVICE,
    )

    embedder = SentenceTransformerEmbedder(
        model=EMBEDDING_MODEL,
        device=EMBEDDING_DEVICE,
    )

    # ------------------------------------------------------------------
    # Stage 5: In-Memory Vector Store (Live-Sync)
    # ------------------------------------------------------------------
    # Unlike external vector databases (Pinecone, Weaviate, Chroma),
    # Pathway's VectorStoreServer is part of the same unified computation
    # graph. Index updates are applied atomically — no eventual consistency,
    # no separate indexing jobs, no knowledge gaps.
    vector_store = VectorStoreServer(
        documents,
        embedder=embedder,
    )

    return vector_store


def main() -> None:
    """Entry point: builds the pipeline and starts the vector store server."""

    logger.info("=" * 60)
    logger.info("  Real-Time Compliance RAG — Pathway + Groq")
    logger.info("=" * 60)

    # Validate that credentials file exists before starting
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(
            f"Google credentials not found at '{SERVICE_ACCOUNT_FILE}'. "
            "Download your service account key from Google Cloud Console "
            "and update SERVICE_ACCOUNT_FILE or set GOOGLE_CREDENTIALS_PATH."
        )

    vector_store = build_pipeline()

    logger.info(
        "🚀 VectorStoreServer starting at http://%s:%d",
        SERVER_HOST,
        SERVER_PORT,
    )
    logger.info("Listening for document changes in Google Drive. Press Ctrl+C to stop.")

    # Starts the HTTP server and the Pathway streaming computation.
    # This call blocks — pw.run() keeps the pipeline alive indefinitely.
    vector_store.run_server(host=SERVER_HOST, port=SERVER_PORT)
    pw.run()


if __name__ == "__main__":
    main()
