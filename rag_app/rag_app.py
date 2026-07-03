"""
RAG Application - Real-Time Customer Support
Performs dual-lookup (Iceberg for live order status + Vector DB for policies)
and generates personalized responses using LLM.
"""

import json
import logging
import os
import sys
from typing import Dict, Any, List, Optional
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_settings
from vector_db.setup_vector_db import ChromaVectorDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


class IcebergQueryClient:
    """Client for querying Apache Iceberg tables."""
    
    def __init__(self):
        """Initialize Iceberg client."""
        self.settings = get_settings()
        self.catalog = None
        self.table = None
    
    def connect(self):
        """Connect to Iceberg catalog."""
        try:
            from pyiceberg.catalog.rest import RestCatalog
            from pyiceberg.catalog import Catalog
            import pyarrow as pa
            
            logger.info(f"Connecting to Iceberg catalog at {self.settings.iceberg_catalog_uri}")
            
            # Configure catalog properties
            catalog_properties = {
                "type": "rest",
                "uri": self.settings.iceberg_catalog_uri,
                "warehouse": self.settings.iceberg_warehouse,
                "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                "s3.endpoint": f"http://{self.settings.minio_endpoint}",
                "s3.path-style-access": "true",
                "s3.access-key-id": self.settings.minio_access_key,
                "s3.secret-access-key": self.settings.minio_secret_key,
            }
            
            # Create catalog
            self.catalog = RestCatalog(
                uri=self.settings.iceberg_catalog_uri,
                warehouse=self.settings.iceberg_warehouse,
                **{k: v for k, v in catalog_properties.items() 
                   if k not in ["type", "uri", "warehouse"]}
            )
            
            logger.info("Connected to Iceberg catalog successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to Iceberg: {e}")
            logger.warning("Falling back to mock data mode")
            return False
    
    def get_customer_order(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest order for a specific customer.
        
        Args:
            customer_id: Customer ID to query
            
        Returns:
            Dictionary with order information or None if not found
        """
        try:
            logger.info(f"Querying Iceberg for customer_id: {customer_id}")
            
            # Load table
            table = self.catalog.load_table((
                self.settings.iceberg_namespace,
                self.settings.iceberg_table_name
            ))
            
            # Scan table and filter by customer_id
            # Note: In production, you'd want to use partition filtering
            df = table.scan().to_pandas()
            
            # Filter for the specific customer
            customer_orders = df[df['customer_id'] == customer_id]
            
            if len(customer_orders) == 0:
                logger.warning(f"No orders found for customer_id: {customer_id}")
                return None
            
            # Get the most recent order by timestamp
            customer_orders['timestamp'] = pd.to_datetime(customer_orders['timestamp'])
            latest_order = customer_orders.loc[customer_orders['timestamp'].idxmax()]
            
            # Convert to dictionary
            order_dict = latest_order.to_dict()
            
            logger.info(f"Found order for customer {customer_id}: {order_dict.get('order_id')}")
            return order_dict
            
        except Exception as e:
            logger.error(f"Error querying Iceberg: {e}")
            return None
    
    def get_customer_order_mock(self, customer_id: str) -> Dict[str, Any]:
        """
        Mock implementation for when Iceberg is not available.
        Simulates a delayed order for customer 123.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Mock order dictionary
        """
        logger.info(f"Using mock data for customer_id: {customer_id}")
        
        if customer_id == "123":
            return {
                "order_id": "ORD-A1B2C3D4",
                "customer_id": "123",
                "status": "in_transit",
                "delivery_notes": "Package delayed due to weather conditions. Expected delivery: 3-5 business days.",
                "timestamp": datetime.now().isoformat(),
                "items": [
                    {"product": "Wireless Headphones", "quantity": 1, "price": 79.99},
                    {"product": "USB-C Cable", "quantity": 2, "price": 12.99}
                ],
                "total_amount": 105.97,
                "shipping_address": "123 Main St, Anytown, ST 12345",
                "carrier": "FedEx",
                "tracking_number": "FDX123456789ABC"
            }
        else:
            return {
                "order_id": f"ORD-{customer_id}XYZ",
                "customer_id": customer_id,
                "status": "delivered",
                "delivery_notes": "Package delivered successfully.",
                "timestamp": datetime.now().isoformat(),
                "items": [],
                "total_amount": 0.0,
                "shipping_address": "Unknown",
                "carrier": "UPS",
                "tracking_number": "UPS123456789"
            }


class LLMClient:
    """Client for interacting with LLM (Ollama or OpenAI)."""
    
    def __init__(self):
        """Initialize LLM client."""
        self.settings = get_settings()
        self.provider = self.settings.llm_provider
    
    def generate_response(self, prompt: str, context: str) -> str:
        """
        Generate response using LLM.
        
        Args:
            prompt: User query
            context: Retrieved context from Iceberg and Vector DB
            
        Returns:
            Generated response
        """
        logger.info(f"Generating response using {self.provider}...")
        
        if self.provider == "ollama":
            return self._generate_ollama(prompt, context)
        elif self.provider == "openai":
            return self._generate_openai(prompt, context)
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")
    
    def _generate_ollama(self, prompt: str, context: str) -> str:
        """
        Generate response using Ollama (local LLM).
        
        Args:
            prompt: User query
            context: Retrieved context
            
        Returns:
            Generated response
        """
        try:
            import requests
            
            # Construct the full prompt
            system_prompt = """You are a helpful customer support assistant for an e-commerce company. 
Use the provided context to answer the customer's question accurately and professionally.
If the context contains information about delays or issues, be empathetic and provide clear next steps.
Always reference specific policies when relevant."""
            
            full_prompt = f"""{system_prompt}

Context from order database and support policies:
{context}

Customer Question: {prompt}

Provide a helpful, personalized response:"""
            
            # Call Ollama API
            response = requests.post(
                f"{self.settings.ollama_base_url}/api/generate",
                json={
                    "model": self.settings.ollama_model,
                    "prompt": full_prompt,
                    "stream": False,
                    "options": {
                        "temperature": self.settings.ollama_temperature,
                        "max_tokens": self.settings.ollama_max_tokens
                    }
                },
                timeout=60
            )
            
            response.raise_for_status()
            result = response.json()
            
            generated_text = result.get('response', '')
            logger.info(f"Generated response ({len(generated_text)} chars)")
            return generated_text
            
        except Exception as e:
            logger.error(f"Error calling Ollama: {e}")
            return f"I apologize, but I'm currently unable to generate a response. Please try again later. (Error: {str(e)})"
    
    def _generate_openai(self, prompt: str, context: str) -> str:
        """
        Generate response using OpenAI API.
        
        Args:
            prompt: User query
            context: Retrieved context
            
        Returns:
            Generated response
        """
        try:
            from openai import OpenAI
            
            if not self.settings.openai_api_key:
                raise ValueError("OpenAI API key not configured. Set OPENAI_API_KEY environment variable.")
            
            client = OpenAI(api_key=self.settings.openai_api_key)
            
            system_prompt = """You are a helpful customer support assistant for an e-commerce company. 
Use the provided context to answer the customer's question accurately and professionally.
If the context contains information about delays or issues, be empathetic and provide clear next steps.
Always reference specific policies when relevant."""
            
            response = client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {prompt}"}
                ],
                temperature=self.settings.openai_temperature,
                max_tokens=self.settings.openai_max_tokens
            )
            
            generated_text = response.choices[0].message.content
            logger.info(f"Generated response ({len(generated_text)} chars)")
            return generated_text
            
        except Exception as e:
            logger.error(f"Error calling OpenAI: {e}")
            return f"I apologize, but I'm currently unable to generate a response. Please try again later. (Error: {str(e)})"


class RAGApplication:
    """Main RAG Application combining Iceberg and Vector DB lookups."""
    
    def __init__(self, use_mock_iceberg: bool = True):
        """
        Initialize RAG application.
        
        Args:
            use_mock_iceberg: If True, use mock data instead of real Iceberg connection
        """
        self.settings = get_settings()
        self.use_mock_iceberg = use_mock_iceberg
        
        # Initialize components
        self.iceberg_client = IcebergQueryClient()
        self.vector_db = ChromaVectorDB()
        self.llm_client = LLMClient()
        
        logger.info("RAG Application initialized")
    
    def initialize(self):
        """Initialize all components."""
        logger.info("Initializing RAG Application components...")
        
        # Connect to Vector DB
        self.vector_db.connect()
        self.vector_db.create_collection()
        
        # Try to connect to Iceberg (fall back to mock if fails)
        if not self.use_mock_iceberg:
            iceberg_connected = self.iceberg_client.connect()
            self.use_mock_iceberg = not iceberg_connected
        
        logger.info(f"Using {'mock' if self.use_mock_iceberg else 'real'} Iceberg data")
        logger.info("RAG Application initialization complete")
    
    def query_customer_order(self, customer_id: str) -> Dict[str, Any]:
        """
        Query customer's latest order from Iceberg.
        
        Args:
            customer_id: Customer ID
            
        Returns:
            Order information dictionary
        """
        if self.use_mock_iceberg:
            return self.iceberg_client.get_customer_order_mock(customer_id)
        else:
            return self.iceberg_client.get_customer_order(customer_id)
    
    def query_policies(self, query: str, top_k: int = None) -> List[Dict[str, Any]]:
        """
        Query Vector DB for relevant policies.
        
        Args:
            query: Search query
            top_k: Number of results to return
            
        Returns:
            List of relevant policy documents
        """
        top_k = top_k or self.settings.rag_top_k
        return self.vector_db.search(query, top_k=top_k)
    
    def build_context(self, order_data: Dict[str, Any], policy_results: List[Dict[str, Any]]) -> str:
        """
        Build context string from order data and policy results.
        
        Args:
            order_data: Order information from Iceberg
            policy_results: Policy documents from Vector DB
            
        Returns:
            Formatted context string
        """
        context_parts = []
        
        # Add order information
        context_parts.append("=== CUSTOMER ORDER INFORMATION ===")
        context_parts.append(f"Order ID: {order_data.get('order_id', 'N/A')}")
        context_parts.append(f"Customer ID: {order_data.get('customer_id', 'N/A')}")
        context_parts.append(f"Order Status: {order_data.get('status', 'N/A')}")
        context_parts.append(f"Delivery Notes: {order_data.get('delivery_notes', 'N/A')}")
        context_parts.append(f"Carrier: {order_data.get('carrier', 'N/A')}")
        context_parts.append(f"Tracking Number: {order_data.get('tracking_number', 'N/A')}")
        context_parts.append(f"Total Amount: ${order_data.get('total_amount', 0.0):.2f}")
        context_parts.append(f"Order Timestamp: {order_data.get('timestamp', 'N/A')}")
        
        if order_data.get('items'):
            context_parts.append("\nOrder Items:")
            for item in order_data['items']:
                context_parts.append(f"  - {item.get('product', 'Unknown')} x{item.get('quantity', 1)} @ ${item.get('price', 0.0):.2f}")
        
        # Add policy information
        context_parts.append("\n=== RELEVANT SUPPORT POLICIES ===")
        for i, policy in enumerate(policy_results, 1):
            context_parts.append(f"\nPolicy {i}:")
            context_parts.append(policy['document'])
        
        return "\n".join(context_parts)
    
    def generate_response(self, customer_query: str, customer_id: str) -> Dict[str, Any]:
        """
        Generate personalized response for customer query.
        
        Args:
            customer_query: Customer's question
            customer_id: Customer ID
            
        Returns:
            Dictionary with response and metadata
        """
        logger.info(f"Processing query for customer {customer_id}: {customer_query}")
        
        # Step 1: Query Iceberg for customer's order
        logger.info("Step 1: Querying order database...")
        order_data = self.query_customer_order(customer_id)
        
        if not order_data:
            return {
                "response": "I'm sorry, but I couldn't find any order information for your account. Please verify your customer ID or contact support for assistance.",
                "order_data": None,
                "policy_context": [],
                "error": "No order found"
            }
        
        # Step 2: Query Vector DB for relevant policies
        logger.info("Step 2: Querying policy database...")
        policy_results = self.query_policies(customer_query)
        
        # Step 3: Build context
        logger.info("Step 3: Building context...")
        context = self.build_context(order_data, policy_results)
        
        # Step 4: Generate response using LLM
        logger.info("Step 4: Generating response...")
        response = self.llm_client.generate_response(customer_query, context)
        
        return {
            "response": response,
            "order_data": order_data,
            "policy_context": [p['document'] for p in policy_results],
            "context_used": context,
            "customer_id": customer_id,
            "timestamp": datetime.now().isoformat()
        }
    
    def interactive_mode(self):
        """Run interactive mode for testing."""
        logger.info("=" * 80)
        logger.info("RAG Application - Interactive Mode")
        logger.info("=" * 80)
        logger.info("Enter customer queries (format: 'Customer <id> asking: <question>')")
        logger.info("Example: 'Customer 123 asking: Why is my order delayed?'")
        logger.info("Type 'quit' or 'exit' to stop\n")
        
        while True:
            try:
                user_input = input("\nQuery: ").strip()
                
                if not user_input:
                    continue
                
                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("Exiting...")
                    break
                
                # Parse customer ID and query
                if "Customer" in user_input and "asking:" in user_input:
                    parts = user_input.split("asking:")
                    customer_part = parts[0].strip()
                    query_part = parts[1].strip()
                    
                    # Extract customer ID
                    customer_id = customer_part.replace("Customer", "").strip()
                    
                    # Generate response
                    result = self.generate_response(query_part, customer_id)
                    
                    # Display results
                    print("\n" + "=" * 80)
                    print("RESPONSE:")
                    print("=" * 80)
                    print(result['response'])
                    print("\n" + "=" * 80)
                    print("ORDER DATA:")
                    print("=" * 80)
                    if result['order_data']:
                        print(f"Order ID: {result['order_data'].get('order_id')}")
                        print(f"Status: {result['order_data'].get('status')}")
                        print(f"Delivery Notes: {result['order_data'].get('delivery_notes')}")
                    print("=" * 80)
                    
                else:
                    print("Invalid format. Use: 'Customer <id> asking: <question>'")
                    
            except KeyboardInterrupt:
                print("\n\nExiting...")
                break
            except Exception as e:
                logger.error(f"Error processing query: {e}", exc_info=True)
                print(f"Error: {e}")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Real-Time Customer Support RAG Application")
    parser.add_argument("--customer-id", type=str, help="Customer ID for single query")
    parser.add_argument("--query", type=str, help="Customer query")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--no-mock", action="store_true", help="Use real Iceberg instead of mock data")
    
    args = parser.parse_args()
    
    # Initialize application
    app = RAGApplication(use_mock_iceberg=not args.no_mock)
    app.initialize()
    
    if args.interactive:
        # Interactive mode
        app.interactive_mode()
    
    elif args.customer_id and args.query:
        # Single query mode
        result = app.generate_response(args.query, args.customer_id)
        
        print("\n" + "=" * 80)
        print("CUSTOMER SUPPORT RESPONSE")
        print("=" * 80)
        print(f"\nCustomer ID: {result['customer_id']}")
        print(f"Timestamp: {result['timestamp']}")
        print("\n" + "-" * 80)
        print("RESPONSE:")
        print("-" * 80)
        print(result['response'])
        print("\n" + "-" * 80)
        print("ORDER DETAILS:")
        print("-" * 80)
        if result['order_data']:
            print(f"Order ID: {result['order_data'].get('order_id')}")
            print(f"Status: {result['order_data'].get('status')}")
            print(f"Carrier: {result['order_data'].get('carrier')}")
            print(f"Tracking: {result['order_data'].get('tracking_number')}")
            print(f"Delivery Notes: {result['order_data'].get('delivery_notes')}")
        print("=" * 80)
    
    else:
        # Default: run interactive mode
        logger.info("No specific query provided. Starting interactive mode...")
        app.interactive_mode()


if __name__ == "__main__":
    # Import pandas here to avoid issues if not installed
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas not installed. Some features may not work.")
    
    main()