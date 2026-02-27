"""
answerer.py — Compliance Query Client
======================================
Connects to the running Pathway VectorStoreServer, retrieves semantically
relevant document chunks for a given question, and generates a formal
compliance response via Groq's Llama-3.3-70B.

Usage:
    python src/answerer.py

Requires:
    - main.py server running at http://127.0.0.1:8000
    - GROQ_API_KEY set in .env
"""

import logging
import os
from typing import Optional

import requests
from dotenv import load_dotenv
from groq import Groq

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

load_dotenv()

# Groq API client — reads GROQ_API_KEY from environment automatically
client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

# Pathway VectorStoreServer endpoint
VECTOR_STORE_URL: str = os.getenv(
    "VECTOR_STORE_URL", "http://127.0.0.1:8000/v1/retrieve"
)

# Number of document chunks to retrieve per query (top-k)
TOP_K: int = int(os.getenv("RETRIEVAL_TOP_K", 3))

# Groq model to use for compliance reasoning
GROQ_MODEL: str = "llama-3.3-70b-versatile"

# LLM temperature — low value enforces factual, deterministic responses
LLM_TEMPERATURE: float = 0.1

# System prompt that establishes the "Senior Compliance Officer" persona.
# The constraints here are deliberate: the model is instructed to stay
# grounded in retrieved context, cite sources, and use formal language.
SYSTEM_PROMPT: str = (
    "You are a Senior Financial Audit & Compliance Officer with deep expertise "
    "in regulatory frameworks (Basel III/IV, SEC, GDPR, SEBI, FINRA). "
    "Your role is to analyze provided regulatory context and answer compliance "
    "questions with precision and professionalism.\n\n"
    "Guidelines:\n"
    "- Base all answers strictly on the provided context. Do not speculate.\n"
    "- Bold key thresholds, percentages, and deadlines using **markdown**.\n"
    "- If the context is insufficient to answer, state this clearly.\n"
    "- Cite specific document sections or regulation numbers where visible.\n"
    "- Flag any contradictions or ambiguities between source documents.\n"
    "- Use formal, jurisdiction-appropriate language throughout."
)

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
# Core Functions
# ---------------------------------------------------------------------------

def retrieve_context(question: str, top_k: int = TOP_K) -> list[dict]:
    """
    Query the Pathway VectorStoreServer and return the top-k most
    semantically relevant document chunks.

    Args:
        question: Natural language compliance query.
        top_k:    Number of chunks to retrieve.

    Returns:
        List of result dicts, each containing 'text', 'dist', and 'metadata'.

    Raises:
        ConnectionError: If the Pathway server is unreachable.
        ValueError:      If the response format is unexpected.
    """
    payload = {"query": question, "k": top_k}

    try:
        response = requests.post(VECTOR_STORE_URL, json=payload, timeout=10)
        response.raise_for_status()
        results = response.json()
    except requests.exceptions.ConnectionError:
        raise ConnectionError(
            f"Cannot reach VectorStoreServer at {VECTOR_STORE_URL}. "
            "Ensure main.py is running."
        )
    except requests.exceptions.Timeout:
        raise ConnectionError("VectorStoreServer request timed out after 10 seconds.")
    except requests.exceptions.HTTPError as e:
        raise ValueError(f"VectorStoreServer returned an error: {e}")

    if not isinstance(results, list):
        raise ValueError(f"Unexpected response format from vector store: {results}")

    return results


def build_context_string(chunks: list[dict]) -> str:
    """
    Concatenate retrieved chunk texts into a single context block
    for the LLM prompt.

    Args:
        chunks: List of chunk dicts returned by retrieve_context().

    Returns:
        A single string of concatenated chunk texts, separated by blank lines.
    """
    return "\n\n".join(chunk["text"] for chunk in chunks)


def extract_sources(chunks: list[dict]) -> list[str]:
    """
    Extract unique source document names from retrieved chunks.

    Args:
        chunks: List of chunk dicts returned by retrieve_context().

    Returns:
        Deduplicated list of source document filenames.
    """
    return list({chunk["metadata"]["name"] for chunk in chunks})


def generate_compliance_response(question: str, context: str) -> str:
    """
    Send the retrieved context and user question to Groq (Llama-3.3-70B)
    and return the compliance officer's response.

    Args:
        question: The original user question.
        context:  Concatenated text from retrieved document chunks.

    Returns:
        Formatted compliance response string from the LLM.
    """
    completion = client.chat.completions.create(
        model=GROQ_MODEL,
        temperature=LLM_TEMPERATURE,
        messages=[
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": (
                    f"RETRIEVED CONTEXT:\n{context}\n\n"
                    f"COMPLIANCE QUESTION: {question}"
                ),
            },
        ],
    )
    return completion.choices[0].message.content


def get_compliance_answer(
    question: str,
) -> tuple[Optional[list[dict]], str, list[str]]:
    """
    Full query pipeline: retrieve → generate → return.

    Args:
        question: Natural language compliance query.

    Returns:
        A tuple of:
            - chunks (list[dict] | None): Raw retrieval results, or None on error.
            - answer (str):              LLM response or error message string.
            - sources (list[str]):       Source document names cited.
    """
    try:
        chunks = retrieve_context(question)
    except (ConnectionError, ValueError) as e:
        logger.error("Retrieval failed: %s", e)
        return None, str(e), []

    context = build_context_string(chunks)
    sources = extract_sources(chunks)

    try:
        answer = generate_compliance_response(question, context)
    except Exception as e:
        logger.error("LLM generation failed: %s", e)
        return chunks, f"LLM error: {e}", sources

    return chunks, answer, sources


# ---------------------------------------------------------------------------
# Display Helpers
# ---------------------------------------------------------------------------

def display_semantic_results(chunks: list[dict]) -> None:
    """
    Print a ranked summary of retrieved document chunks with similarity scores.
    Similarity is derived from the distance score: similarity = 1 - dist.

    Args:
        chunks: List of chunk dicts from retrieve_context().
    """
    print("\n🧠 TOP-K SEMANTIC SEARCH RESULTS (Evidence)")
    print("─" * 50)
    for rank, chunk in enumerate(chunks, start=1):
        # Convert distance to similarity score (1 = identical, 0 = unrelated)
        similarity = round(1 - chunk["dist"], 4)
        source_name = chunk["metadata"]["name"]
        snippet = chunk["text"][:160].replace("\n", " ")

        print(f"  Rank {rank} | Similarity: {similarity:.4f} | Source: {source_name}")
        print(f"  Snippet: {snippet}...")
        print()


def display_citations(sources: list[str]) -> None:
    """
    Print the list of source documents cited in the response.

    Args:
        sources: Deduplicated list of source document names.
    """
    print("📚 CITATIONS")
    print("─" * 50)
    for source in sources:
        print(f"  ✅ {source}")


# ---------------------------------------------------------------------------
# Interactive Demo
# ---------------------------------------------------------------------------

def run_demo() -> None:
    """
    Interactive REPL loop for querying the compliance agent.
    Walks through retrieval, LLM generation, and displays all evidence.
    """
    print("\n" + "=" * 60)
    print("  🏦 PATHWAY + GROQ: REAL-TIME COMPLIANCE AGENT")
    print("=" * 60)
    print("Connected to live Google Drive. Monitoring regulatory changes.")
    print("Type 'exit' or 'quit' to shut down.\n")

    while True:
        print("─" * 60)

        try:
            query = input("🤖 Query: ").strip()
        except (KeyboardInterrupt, EOFError):
            # Handle Ctrl+C or piped input ending gracefully
            print("\nShutting down compliance agent. Goodbye!")
            break

        if not query:
            continue

        if query.lower() in {"exit", "quit"}:
            print("Shutting down compliance agent. Goodbye!")
            break

        # ------------------------------------------------------------------
        # Step 1: Semantic Retrieval
        # ------------------------------------------------------------------
        print("\n🔍 [1/3] Querying Pathway vector store...")
        chunks, answer, sources = get_compliance_answer(query)

        # If retrieval failed entirely, print the error and continue
        if chunks is None:
            print(f"\n❌ Error: {answer}")
            continue

        # ------------------------------------------------------------------
        # Step 2: Compliance Report from LLM
        # ------------------------------------------------------------------
        print("\n📋 [2/3] COMPLIANCE REPORT")
        print("─" * 50)
        print(answer)

        # ------------------------------------------------------------------
        # Step 3: Evidence & Citations
        # ------------------------------------------------------------------
        print(f"\n[3/3] EVIDENCE & CITATIONS")
        display_semantic_results(chunks)
        display_citations(sources)


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_demo()
