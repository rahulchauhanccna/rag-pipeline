"""
Test Script for Real-Time Customer Support RAG Pipeline
Validates all components and their integration.
"""

import sys
import os
import time
import json
import logging
from pathlib import Path

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.settings import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


class TestResult:
    """Stores test results."""
    
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.passed = False
        self.message = ""
        self.duration = 0.0
    
    def success(self, message: str):
        """Mark test as passed."""
        self.passed = True
        self.message = message
        logger.info(f"✓ {self.test_name}: {message}")
    
    def failure(self, message: str):
        """Mark test as failed."""
        self.passed = False
        self.message = message
        logger.error(f"✗ {self.test_name}: {message}")
    
    def __str__(self):
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.test_name}: {self.message}"


class PipelineTester:
    """Test suite for the RAG pipeline."""
    
    def __init__(self):
        """Initialize tester."""
        self.results: list[TestResult] = []
        self.start_time = time.time()
    
    def run_test(self, test_name: str, test_func):
        """
        Run a single test.
        
        Args:
            test_name: Name of the test
            test_func: Function to execute
        """
        logger.info(f"\nRunning test: {test_name}")
        result = TestResult(test_name)
        
        start = time.time()
        try:
            test_func(result)
        except Exception as e:
            result.failure(f"Exception: {str(e)}")
        
        result.duration = time.time() - start
        self.results.append(result)
    
    def test_docker_services(self, result: TestResult):
        """Test if Docker services are running."""
        import subprocess
        
        services = {
            "minio": "localhost:9000",
            "iceberg-rest": "localhost:8181",
            "flink-jobmanager": "localhost:8081",
            "chroma": "localhost:8000"
        }
        
        import requests
        
        failed_services = []
        for service, url in services.items():
            try:
                if service == "minio":
                    response = requests.get(f"http://{url}/minio/health/live", timeout=2)
                elif service == "iceberg-rest":
                    response = requests.get(f"http://{url}/v1/config", timeout=2)
                elif service == "flink-jobmanager":
                    response = requests.get(f"http://{url}/overview", timeout=2)
                elif service == "chroma":
                    response = requests.get(f"http://{url}/", timeout=2)
                
                if response.status_code in [200, 410]:
                    logger.info(f"  ✓ {service} is healthy")
                else:
                    failed_services.append(f"{service} (status: {response.status_code})")
            except Exception as e:
                failed_services.append(f"{service} (error: {str(e)})")
        
        if failed_services:
            result.failure(f"Services not ready: {', '.join(failed_services)}")
        else:
            result.success("All Docker services are running")
    
    def test_python_dependencies(self, result: TestResult):
        """Test if required Python packages are installed."""
        required_packages = {
            "flink": "apache-flink",
            "pyiceberg": "pyiceberg",
            "chromadb": "chromadb",
            "sentence_transformers": "sentence-transformers",
            "langchain": "langchain",
            "faker": "faker",
            "requests": "requests"
        }
        
        missing = []
        for module, package in required_packages.items():
            try:
                if module == "flink":
                    import flink
                else:
                    __import__(module.replace("-", "_"))
                logger.info(f"  ✓ {package} is installed")
            except ImportError:
                missing.append(package)
                logger.warning(f"  ✗ {package} is missing")
        
        if missing:
            result.failure(f"Missing packages: {', '.join(missing)}")
        else:
            result.success("All Python dependencies are installed")
    
    def test_configuration(self, result: TestResult):
        """Test configuration loading."""
        try:
            # Test that settings can be loaded
            assert settings.minio_endpoint is not None
            assert settings.iceberg_catalog_uri is not None
            assert settings.chroma_host is not None
            assert settings.ollama_base_url is not None
            
            logger.info(f"  ✓ MinIO endpoint: {settings.minio_endpoint}")
            logger.info(f"  ✓ Iceberg catalog: {settings.iceberg_catalog_uri}")
            logger.info(f"  ✓ Chroma host: {settings.chroma_host}")
            logger.info(f"  ✓ LLM provider: {settings.llm_provider}")
            
            result.success("Configuration loaded successfully")
        except Exception as e:
            result.failure(f"Configuration error: {str(e)}")
    
    def test_order_stream_generator(self, result: TestResult):
        """Test order stream generator."""
        try:
            from data_generators.order_stream_generator import OrderStreamGenerator
            
            # Create generator
            generator = OrderStreamGenerator(customer_123_delayed=True)
            
            # Generate test events
            events = []
            for _ in range(5):
                event = generator.generate_order_event()
                events.append(event)
            
            # Validate events
            assert len(events) == 5, "Should generate 5 events"
            
            for event in events:
                assert "order_id" in event, "Event should have order_id"
                assert "customer_id" in event, "Event should have customer_id"
                assert "status" in event, "Event should have status"
                assert "timestamp" in event, "Event should have timestamp"
            
            # Check customer 123's delayed order exists
            customer_123_events = [e for e in events if e.get("customer_id") == "123"]
            logger.info(f"  ✓ Generated {len(events)} events")
            logger.info(f"  ✓ Customer 123 events: {len(customer_123_events)}")
            
            result.success(f"Generated and validated {len(events)} order events")
        except Exception as e:
            result.failure(f"Order generator error: {str(e)}")
    
    def test_vector_db_connection(self, result: TestResult):
        """Test Chroma Vector DB connection."""
        try:
            import chromadb
            import requests
            
            # Test connection
            response = requests.get(f"http://{settings.chroma_host}:{settings.chroma_port}/api/v1/heartbeat", timeout=2)
            
            if response.status_code == 200:
                # Try to create client
                client = chromadb.HttpClient(
                    host=settings.chroma_host,
                    port=settings.chroma_port
                )
                
                # List collections
                collections = client.list_collections()
                logger.info(f"  ✓ Connected to Chroma")
                logger.info(f"  ✓ Found {len(collections)} collections")
                
                result.success("Chroma Vector DB connection successful")
            else:
                result.failure(f"Chroma returned status {response.status_code}")
        except Exception as e:
            result.failure(f"Connection error: {str(e)}")
    
    def test_vector_db_search(self, result: TestResult):
        """Test vector database search functionality."""
        try:
            from vector_db.setup_vector_db import ChromaVectorDB
            
            vector_db = ChromaVectorDB()
            vector_db.connect()
            vector_db.create_collection()
            
            # Test search
            test_query = "What is the refund policy?"
            search_results = vector_db.search(test_query, top_k=2)
            
            logger.info(f"  ✓ Search query: '{test_query}'")
            logger.info(f"  ✓ Found {len(search_results)} results")
            
            if len(search_results) > 0:
                logger.info(f"  ✓ Top result: {search_results[0]['document'][:80]}...")
                result.success(f"Vector search working ({len(search_results)} results)")
            else:
                result.failure("No search results found - collection may be empty")
        except Exception as e:
            result.failure(f"Search error: {str(e)}")
    
    def test_iceberg_connection(self, result: TestResult):
        """Test Iceberg catalog connection."""
        try:
            from pyiceberg.catalog.rest import RestCatalog
            
            # Try to connect
            catalog = RestCatalog(
                name="test_catalog",
                uri=settings.iceberg_catalog_uri,
                warehouse=settings.iceberg_warehouse
            )
            
            # List namespaces
            namespaces = catalog.list_namespaces()
            logger.info(f"  ✓ Connected to Iceberg catalog")
            logger.info(f"  ✓ Found {len(namespaces)} namespaces")
            
            result.success("Iceberg catalog connection successful")
        except Exception as e:
            result.failure(f"Connection error: {str(e)}")
    
    def test_iceberg_table(self, result: TestResult):
        """Test Iceberg table existence and structure."""
        try:
            from pyiceberg.catalog.rest import RestCatalog
            
            catalog = RestCatalog(
                name="test_catalog",
                uri=settings.iceberg_catalog_uri,
                warehouse=settings.iceberg_warehouse
            )
            
            # Check if table exists
            table_exists = catalog.table_exists((
                settings.iceberg_namespace,
                settings.iceberg_table_name
            ))
            
            if table_exists:
                table = catalog.load_table((
                    settings.iceberg_namespace,
                    settings.iceberg_table_name
                ))
                
                schema = table.schema()
                logger.info(f"  ✓ Table exists: {settings.iceberg_table_name}")
                logger.info(f"  ✓ Schema has {len(schema.columns)} columns")
                
                result.success("Iceberg table exists and is accessible")
            else:
                result.failure(f"Table {settings.iceberg_table_name} does not exist")
        except Exception as e:
            result.failure(f"Error: {str(e)}")
    
    def test_rag_application(self, result: TestResult):
        """Test RAG application initialization."""
        try:
            from rag_app.rag_app import RAGApplication
            
            # Initialize app
            app = RAGApplication(use_mock_iceberg=True)
            app.initialize()
            
            logger.info(f"  ✓ RAG Application initialized")
            logger.info(f"  ✓ Using mock Iceberg: {app.use_mock_iceberg}")
            
            # Test single query
            test_result = app.generate_response(
                customer_query="What is your refund policy?",
                customer_id="123"
            )
            
            assert "response" in test_result, "Result should have response"
            assert "order_data" in test_result, "Result should have order_data"
            assert len(test_result["response"]) > 0, "Response should not be empty"
            
            logger.info(f"  ✓ Generated test response ({len(test_result['response'])} chars)")
            logger.info(f"  ✓ Order data retrieved: {test_result['order_data']['order_id']}")
            
            result.success("RAG Application working correctly")
        except Exception as e:
            result.failure(f"Error: {str(e)}")
    
    def test_end_to_end_query(self, result: TestResult):
        """Test complete end-to-end query."""
        try:
            from rag_app.rag_app import RAGApplication
            
            app = RAGApplication(use_mock_iceberg=True)
            app.initialize()
            
            # Simulate customer query
            customer_id = "123"
            query = "Why is my order delayed and what is your refund policy?"
            
            logger.info(f"  Testing query: Customer {customer_id}")
            logger.info(f"  Query: {query}")
            
            result_data = app.generate_response(query, customer_id)
            
            # Validate result
            assert result_data["response"], "Response should not be empty"
            assert result_data["order_data"], "Order data should exist"
            assert result_data["policy_context"], "Policy context should exist"
            
            logger.info(f"  ✓ Response generated ({len(result_data['response'])} chars)")
            logger.info(f"  ✓ Order: {result_data['order_data']['order_id']}")
            logger.info(f"  ✓ Policies retrieved: {len(result_data['policy_context'])}")
            
            result.success("End-to-end query successful")
        except Exception as e:
            result.failure(f"Error: {str(e)}")
    
    def print_summary(self):
        """Print test summary."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        total_duration = time.time() - self.start_time
        
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"Total Tests: {total}")
        print(f"Passed: {passed} ({passed/total*100:.1f}%)")
        print(f"Failed: {failed} ({failed/total*100:.1f}%)")
        print(f"Total Duration: {total_duration:.2f}s")
        print("=" * 80)
        
        if failed > 0:
            print("\nFailed Tests:")
            print("-" * 80)
            for result in self.results:
                if not result.passed:
                    print(result)
        
        print("\nAll Tests:")
        print("-" * 80)
        for result in self.results:
            print(result)
        
        print("=" * 80)
        
        return failed == 0
    
    def run_all_tests(self):
        """Run all tests."""
        logger.info("=" * 80)
        logger.info("Starting RAG Pipeline Test Suite")
        logger.info("=" * 80)
        
        # Run tests
        self.run_test("Docker Services", self.test_docker_services)
        self.run_test("Python Dependencies", self.test_python_dependencies)
        self.run_test("Configuration", self.test_configuration)
        self.run_test("Order Stream Generator", self.test_order_stream_generator)
        self.run_test("Vector DB Connection", self.test_vector_db_connection)
        self.run_test("Vector DB Search", self.test_vector_db_search)
        self.run_test("Iceberg Connection", self.test_iceberg_connection)
        self.run_test("Iceberg Table", self.test_iceberg_table)
        self.run_test("RAG Application", self.test_rag_application)
        self.run_test("End-to-End Query", self.test_end_to_end_query)
        
        # Print summary
        all_passed = self.print_summary()
        
        return 0 if all_passed else 1


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test RAG Pipeline Components")
    parser.add_argument("--test", choices=[
        "docker", "deps", "config", "generator", "vector-db", 
        "iceberg", "rag", "e2e", "all"
    ], default="all", help="Test to run (default: all)")
    
    args = parser.parse_args()
    
    tester = PipelineTester()
    
    # Run specific test or all tests
    if args.test == "all":
        exit_code = tester.run_all_tests()
    else:
        test_map = {
            "docker": ("Docker Services", tester.test_docker_services),
            "deps": ("Python Dependencies", tester.test_python_dependencies),
            "config": ("Configuration", tester.test_configuration),
            "generator": ("Order Stream Generator", tester.test_order_stream_generator),
            "vector-db": ("Vector DB", tester.test_vector_db_connection),
            "iceberg": ("Iceberg", tester.test_iceberg_connection),
            "rag": ("RAG Application", tester.test_rag_application),
            "e2e": ("End-to-End Query", tester.test_end_to_end_query)
        }
        
        test_name, test_func = test_map[args.test]
        result = TestResult(test_name)
        
        try:
            test_func(result)
            print(result)
            exit_code = 0 if result.passed else 1
        except Exception as e:
            result.failure(f"Exception: {str(e)}")
            print(result)
            exit_code = 1
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()