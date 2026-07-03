"""
Vector Database Setup Script
Chunks policy documents, generates embeddings, and stores them in Chroma Vector DB.
"""

import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import re

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_settings
from langchain.text_splitter import MarkdownTextSplitter, RecursiveCharacterTextSplitter
from langchain_community.document_loaders import TextLoader
from langchain.schema import Document
from sentence_transformers import SentenceTransformer
import chromadb
from chromadb.config import Settings as ChromaSettings
from chromadb.utils import embedding_functions

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


class PolicyDocumentLoader:
    """Loads and processes policy documents."""
    
    def __init__(self, policy_file_path: str):
        """
        Initialize the document loader.
        
        Args:
            policy_file_path: Path to the policy markdown file
        """
        self.policy_file_path = policy_file_path
        self.documents: List[Document] = []
    
    def load_documents(self) -> List[Document]:
        """
        Load policy documents from markdown file.
        
        Returns:
            List of Document objects
        """
        logger.info(f"Loading policy documents from: {self.policy_file_path}")
        
        if not os.path.exists(self.policy_file_path):
            raise FileNotFoundError(f"Policy file not found: {self.policy_file_path}")
        
        # Use MarkdownLoader for better markdown parsing
        loader = TextLoader(self.policy_file_path, encoding='utf-8')
        self.documents = loader.load()
        
        logger.info(f"Loaded {len(self.documents)} document(s)")
        return self.documents
    
    def chunk_documents(self, chunk_size: int = 500, chunk_overlap: int = 50) -> List[Document]:
        """
        Chunk documents into smaller pieces for better retrieval.
        
        Args:
            chunk_size: Maximum size of each chunk
            chunk_overlap: Overlap between chunks
            
        Returns:
            List of chunked Document objects
        """
        logger.info(f"Chunking documents (size={chunk_size}, overlap={chunk_overlap})...")
        
        if not self.documents:
            self.load_documents()
        
        # Use MarkdownTextSplitter for markdown-aware chunking
        text_splitter = MarkdownTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap
        )
        
        chunks = text_splitter.split_documents(self.documents)
        
        # Add metadata to each chunk
        for i, chunk in enumerate(chunks):
            chunk.metadata['chunk_id'] = i
            chunk.metadata['source'] = self.policy_file_path
        
        logger.info(f"Created {len(chunks)} chunks from {len(self.documents)} documents")
        return chunks


class EmbeddingGenerator:
    """Generates embeddings using sentence-transformers."""
    
    def __init__(self, model_name: str = None):
        """
        Initialize the embedding generator.
        
        Args:
            model_name: Name of the sentence-transformers model
        """
        self.model_name = model_name or settings.embedding_model_name
        self.model = None
        logger.info(f"Initializing embedding model: {self.model_name}")
    
    def load_model(self):
        """Load the embedding model."""
        if self.model is None:
            logger.info(f"Loading embedding model: {self.model_name}")
            self.model = SentenceTransformer(self.model_name)
            logger.info("Embedding model loaded successfully")
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of text strings
            
        Returns:
            List of embedding vectors
        """
        if self.model is None:
            self.load_model()
        
        logger.info(f"Generating embeddings for {len(texts)} texts...")
        embeddings = self.model.encode(texts, show_progress_bar=True)
        
        # Convert to list of lists for Chroma
        embeddings_list = embeddings.tolist()
        
        logger.info(f"Generated {len(embeddings_list)} embeddings")
        return embeddings_list
    
    def get_embedding_dimension(self) -> int:
        """
        Get the dimension of the embedding model.
        
        Returns:
            Embedding dimension
        """
        if self.model is None:
            self.load_model()
        return self.model.get_sentence_embedding_dimension()


class ChromaVectorDB:
    """Chroma Vector Database wrapper."""
    
    def __init__(self, host: str = None, port: int = None, collection_name: str = None):
        """
        Initialize Chroma Vector DB.
        
        Args:
            host: Chroma server host
            port: Chroma server port
            collection_name: Name of the collection
        """
        self.host = host or settings.chroma_host
        self.port = port or settings.chroma_port
        self.collection_name = collection_name or settings.chroma_collection_name
        self.client = None
        self.collection = None
        self.embedding_generator = EmbeddingGenerator()
    
    def connect(self):
        """Connect to Chroma server."""
        logger.info(f"Connecting to Chroma at {self.host}:{self.port}...")
        
        try:
            # Initialize Chroma client
            self.client = chromadb.HttpClient(
                host=self.host,
                port=self.port,
                settings=ChromaSettings(allow_reset=True)
            )
            
            # Test connection
            self.client.heartbeat()
            logger.info("Connected to Chroma successfully")
            
        except Exception as e:
            logger.error(f"Failed to connect to Chroma: {e}")
            logger.info("Make sure Chroma is running (docker-compose up chroma)")
            raise
    
    def create_collection(self, reset: bool = False):
        """
        Create or get collection.
        
        Args:
            reset: If True, delete existing collection and create new one
        """
        if self.client is None:
            self.connect()
        
        # Reset collection if requested
        if reset:
            logger.info(f"Deleting existing collection: {self.collection_name}")
            try:
                self.client.delete_collection(self.collection_name)
            except Exception:
                pass
        
        # Create or get collection
        logger.info(f"Creating/getting collection: {self.collection_name}")
        
        # Use sentence-transformers embedding function
        embedding_function = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=settings.embedding_model_name
        )
        
        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            embedding_function=embedding_function,
            metadata={"hnsw:space": "cosine"}
        )
        
        logger.info(f"Collection '{self.collection_name}' ready")
    
    def add_documents(self, chunks: List[Document], batch_size: int = 100):
        """
        Add document chunks to the collection.
        
        Args:
            chunks: List of Document chunks
            batch_size: Number of documents to add in each batch
        """
        if self.collection is None:
            self.create_collection()
        
        logger.info(f"Adding {len(chunks)} chunks to collection...")
        
        # Prepare data for Chroma
        ids = []
        documents = []
        metadatas = []
        
        for chunk in chunks:
            chunk_id = f"chunk_{chunk.metadata.get('chunk_id', len(ids))}"
            ids.append(chunk_id)
            documents.append(chunk.page_content)
            
            # Prepare metadata (Chroma requires simple types)
            metadata = {
                'source': chunk.metadata.get('source', ''),
                'chunk_id': str(chunk.metadata.get('chunk_id', ''))
            }
            metadatas.append(metadata)
        
        # Add in batches
        for i in range(0, len(ids), batch_size):
            batch_ids = ids[i:i + batch_size]
            batch_documents = documents[i:i + batch_size]
            batch_metadatas = metadatas[i:i + batch_size]
            
            logger.info(f"Adding batch {i // batch_size + 1}/{(len(ids) + batch_size - 1) // batch_size}")
            
            self.collection.add(
                ids=batch_ids,
                documents=batch_documents,
                metadatas=batch_metadatas
            )
        
        logger.info(f"Successfully added {len(chunks)} chunks to collection")
    
    def search(self, query: str, top_k: int = None, similarity_threshold: float = None) -> List[Dict[str, Any]]:
        """
        Search for similar documents.
        
        Args:
            query: Search query
            top_k: Number of results to return
            similarity_threshold: Minimum similarity score
            
        Returns:
            List of search results with documents and scores
        """
        if self.collection is None:
            self.create_collection()
        
        top_k = top_k or settings.rag_top_k
        similarity_threshold = similarity_threshold or settings.rag_similarity_threshold
        
        logger.info(f"Searching for: '{query}' (top_k={top_k})")
        
        # Query the collection
        results = self.collection.query(
            query_texts=[query],
            n_results=top_k
        )
        
        # Format results
        formatted_results = []
        if results['ids'] and len(results['ids'][0]) > 0:
            for i in range(len(results['ids'][0])):
                result = {
                    'id': results['ids'][0][i],
                    'document': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i],
                    'distance': results['distances'][0][i] if 'distances' in results else None
                }
                formatted_results.append(result)
        
        logger.info(f"Found {len(formatted_results)} results")
        return formatted_results
    
    def get_collection_info(self) -> Dict[str, Any]:
        """
        Get information about the collection.
        
        Returns:
            Dictionary with collection information
        """
        if self.collection is None:
            self.create_collection()
        
        count = self.collection.count()
        return {
            'name': self.collection_name,
            'count': count,
            'metadata': self.collection.metadata
        }


def setup_vector_db(reset: bool = False):
    """
    Main function to setup the vector database.
    
    Args:
        reset: If True, reset the collection before adding documents
    """
    logger.info("=" * 80)
    logger.info("Vector Database Setup")
    logger.info("=" * 80)
    
    # Path to policy documents
    policy_file = os.path.join(
        os.path.dirname(__file__),
        "..",
        "data-generators",
        "policy_documents.md"
    )
    
    # Load and chunk documents
    loader = PolicyDocumentLoader(policy_file)
    chunks = loader.chunk_documents(chunk_size=500, chunk_overlap=50)
    
    # Setup Chroma
    vector_db = ChromaVectorDB()
    vector_db.create_collection(reset=reset)
    
    # Add documents to vector DB
    vector_db.add_documents(chunks)
    
    # Print collection info
    info = vector_db.get_collection_info()
    logger.info("=" * 80)
    logger.info("Vector Database Setup Complete")
    logger.info(f"Collection: {info['name']}")
    logger.info(f"Total chunks: {info['count']}")
    logger.info("=" * 80)
    
    # Test search
    logger.info("\nTesting search functionality...")
    test_queries = [
        "What is the refund policy?",
        "How long does shipping take?",
        "What happens if my package is delayed?"
    ]
    
    for query in test_queries:
        logger.info(f"\nQuery: {query}")
        results = vector_db.search(query, top_k=2)
        for i, result in enumerate(results, 1):
            logger.info(f"  Result {i}: {result['document'][:100]}...")
    
    return vector_db


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Setup Vector Database with Policy Documents")
    parser.add_argument("--reset", action="store_true", help="Reset collection before adding documents")
    parser.add_argument("--test", action="store_true", help="Run test queries after setup")
    
    args = parser.parse_args()
    
    try:
        vector_db = setup_vector_db(reset=args.reset)
        
        if args.test:
            logger.info("\n" + "=" * 80)
            logger.info("Interactive Test Mode")
            logger.info("=" * 80)
            logger.info("Enter queries to search the policy database (Ctrl+C to exit):\n")
            
            while True:
                try:
                    query = input("\nQuery: ").strip()
                    if not query:
                        continue
                    
                    results = vector_db.search(query, top_k=3)
                    print(f"\nFound {len(results)} results:")
                    for i, result in enumerate(results, 1):
                        print(f"\n{i}. {result['document']}")
                        if result['distance']:
                            print(f"   Distance: {result['distance']:.4f}")
                except KeyboardInterrupt:
                    print("\n\nExiting...")
                    break
    except Exception as e:
        logger.error(f"Error setting up vector database: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()