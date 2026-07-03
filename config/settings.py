"""
Configuration settings for the RAG Pipeline.
Centralized configuration for all components.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Application
    app_name: str = "Real-Time Customer Support RAG Pipeline"
    debug: bool = True
    log_level: str = "INFO"
    
    # MinIO Configuration
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "password123"
    minio_bucket: str = "warehouse"
    minio_region: str = "us-east-1"
    
    # Iceberg REST Catalog
    iceberg_catalog_uri: str = "http://localhost:8181"
    iceberg_catalog_name: str = "rag_catalog"
    iceberg_warehouse: str = "s3://warehouse"
    iceberg_namespace: str = "ecommerce"
    iceberg_table_name: str = "orders"
    
    # Flink Configuration
    flink_jobmanager_host: str = "localhost"
    flink_jobmanager_port: int = 8081
    
    # Chroma Vector DB
    chroma_host: str = "localhost"
    chroma_port: int = 8000
    chroma_collection_name: str = "support_policies"
    
    # Embedding Model
    embedding_model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    embedding_dimension: int = 384
    
    # LLM Configuration
    llm_provider: str = Field(default="ollama", description="LLM provider: 'ollama' or 'openai'")
    
    # Ollama Configuration
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "llama3.1:latest"
    ollama_temperature: float = 0.7
    ollama_max_tokens: int = 500
    
    # OpenAI Configuration (alternative to Ollama)
    openai_api_key: Optional[str] = Field(default=None, description="OpenAI API key")
    openai_model: str = "gpt-3.5-turbo"
    openai_temperature: float = 0.7
    openai_max_tokens: int = 500
    
    # RAG Configuration
    rag_top_k: int = 3
    rag_similarity_threshold: float = 0.7
    
    # Stream Configuration
    stream_batch_size: int = 100
    stream_checkpoint_interval: int = 5000
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings."""
    return settings