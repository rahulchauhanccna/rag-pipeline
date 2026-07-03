# CLINE Rules - Real-Time Customer Support RAG Pipeline

This document defines the development rules, best practices, and guidelines for maintaining and extending this RAG Pipeline project.

## 🎯 Project Philosophy

### Core Principles
1. **Modularity First** - Each component should be independently testable and replaceable
2. **Local-First Development** - Everything must run on a single machine for learning/development
3. **Open-Source Only** - No proprietary dependencies or cloud-only services
4. **Production-Ready Patterns** - Even in prototype, use production-grade code patterns
5. **Comprehensive Documentation** - Code should be self-documenting with clear comments

## 📁 File Organization Rules

### Directory Structure
```
rag-pipeline/
├── config/           # Configuration and settings (NO business logic)
├── data-generators/  # Data generation and mock data
├── flink-job/        # PyFlink streaming jobs
├── vector-db/        # Vector database operations
├── rag-app/          # Main application logic
└── tests/            # Unit and integration tests (future)
```

### Naming Conventions
- **Python files:** `snake_case.py`
- **Classes:** `PascalCase`
- **Functions/Methods:** `snake_case`
- **Constants:** `UPPER_SNAKE_CASE`
- **Private methods:** `_leading_underscore`

### File Naming Rules
- One class per file (unless tightly coupled)
- File name should match primary class name
- Test files: `test_<module_name>.py`
- Config files: `<component>_config.py` or `settings.py`

## 🐍 Python Code Standards

### Code Style
- Follow **PEP 8** strictly
- Use **type hints** for all function signatures
- Maximum line length: **100 characters**
- Use **docstrings** for all classes and public methods (Google style)

### Example Template
```python
def function_name(param: str, optional: int = 10) -> Dict[str, Any]:
    """
    Brief description of what the function does.
    
    Args:
        param: Description of parameter
        optional: Description of optional parameter
        
    Returns:
        Description of return value
        
    Raises:
        ExceptionType: When this exception is raised
    """
    # Implementation
    pass
```

### Import Organization
```python
# 1. Standard library
import os
import sys
from typing import Dict, List, Optional

# 2. Third-party packages
import requests
from flask import Flask
from langchain.schema import Document

# 3. Local imports (absolute preferred)
from config.settings import get_settings
from vector_db.setup_vector_db import ChromaVectorDB
```

### Error Handling
```python
# Always use specific exceptions
try:
    result = risky_operation()
except SpecificError as e:
    logger.error(f"Operation failed: {e}")
    raise  # Re-raise if can't handle
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    raise CustomError("User-friendly message") from e
```

### Logging Standards
```python
import logging

logger = logging.getLogger(__name__)

# Use appropriate log levels
logger.debug("Detailed info for debugging")
logger.info("General informational messages")
logger.warning("Warning messages")
logger.error("Error messages")
logger.critical("Critical failures")

# Always include context
logger.info(f"Processing order {order_id} for customer {customer_id}")
```

## 🔧 Configuration Management

### Settings Rules
1. **Centralized Configuration** - All settings in `config/settings.py`
2. **Environment Variables** - Use `.env` files, never hardcode
3. **Type Safety** - Use Pydantic for validation
4. **Defaults** - Provide sensible defaults for all settings
5. **Documentation** - Every setting must have a description

### Adding New Settings
```python
# In config/settings.py
new_feature_enabled: bool = Field(
    default=False,
    description="Enable new feature X for testing"
)

# In .env.example
NEW_FEATURE_ENABLED=false
```

## 🗄️ Database Operations

### Iceberg Rules
1. **Partitioning Strategy** - Partition by frequently filtered columns (e.g., customer_id)
2. **Schema Evolution** - Use schema versioning for table changes
3. **Time Travel** - Leverage for debugging and audit trails
4. **Metadata Management** - Always use REST catalog, never direct file access

### Vector DB Rules
1. **Chunking Strategy** - Use markdown-aware splitters for documents
2. **Embedding Consistency** - Same model for ingestion and querying
3. **Metadata Enrichment** - Store source, timestamp, and chunk_id
4. **Batch Operations** - Insert in batches of 100-1000 for performance

### Example: Adding New Table
```python
# In flink-job/flink_job.py
def create_new_table(self):
    """Create new Iceberg table with proper schema."""
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS new_table (
            id STRING,
            customer_id STRING,
            data STRING,
            timestamp STRING,
            ingestion_time TIMESTAMP(3) METADATA FROM 'values.ingestion-time' VIRTUAL
        ) PARTITIONED BY (customer_id)
        WITH (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'snappy'
        )
    """
    self.t_env.execute_sql(create_table_sql)
```

## 🤖 LLM Integration Rules

### Prompt Engineering
1. **System Prompts** - Always define role and behavior
2. **Context Separation** - Clearly separate different data sources
3. **Token Management** - Monitor and limit context size
4. **Fallback Responses** - Always have graceful degradation

### Example Prompt Structure
```python
system_prompt = """You are a [ROLE].

Use the following context to answer:
[CONTEXT_SECTION_1]
[CONTEXT_SECTION_2]

Rules:
1. [Rule 1]
2. [Rule 2]
3. [Rule 3]
"""

user_prompt = f"""Question: {user_question}

Provide a [SPECIFIC_FORMAT] response:"""
```

### LLM Provider Abstraction
```python
class LLMClient:
    """Abstract LLM provider."""
    
    def generate(self, prompt: str, context: str) -> str:
        """Generate response - implement for each provider."""
        raise NotImplementedError

class OllamaClient(LLMClient):
    """Ollama implementation."""
    pass

class OpenAIClient(LLMClient):
    """OpenAI implementation."""
    pass
```

## 🧪 Testing Rules

### Test Coverage Requirements
- **Unit Tests:** All utility functions and classes
- **Integration Tests:** Component interactions (DB, LLM, etc.)
- **End-to-End Tests:** Complete pipeline flows
- **Minimum Coverage:** 80% for core components

### Test Structure
```python
class TestComponent:
    """Test suite for Component."""
    
    def test_basic_functionality(self):
        """Test basic case."""
        result = component.do_something()
        assert result == expected
    
    def test_error_handling(self):
        """Test error cases."""
        with pytest.raises(ValueError):
            component.do_something_invalid()
    
    def test_edge_cases(self):
        """Test boundary conditions."""
        pass
```

### Running Tests
```bash
# All tests
python test_pipeline.py

# Specific test
python test_pipeline.py --test vector-db

# With coverage
pytest --cov=rag-app --cov=vector-db tests/
```

## 🔄 Data Flow Rules

### Stream Processing
1. **Idempotency** - Processing same event twice should be safe
2. **Ordering** - Maintain event order within partitions
3. **Checkpointing** - Enable exactly-once semantics
4. **Backpressure** - Handle slow consumers gracefully

### Data Validation
```python
def validate_order_event(event: Dict[str, Any]) -> bool:
    """Validate order event schema."""
    required_fields = ['order_id', 'customer_id', 'status', 'timestamp']
    
    for field in required_fields:
        if field not in event:
            logger.error(f"Missing required field: {field}")
            return False
    
    # Type validation
    if not isinstance(event['customer_id'], str):
        logger.error("customer_id must be string")
        return False
    
    return True
```

## 📝 Documentation Rules

### Code Documentation
- **Docstrings** for all public classes and methods
- **Inline comments** for complex logic only
- **Type hints** mandatory for all function signatures
- **README** in every major directory if needed

### README Requirements
Every major component must have:
1. **Purpose** - What it does
2. **Usage** - How to use it
3. **Examples** - Code examples
4. **Troubleshooting** - Common issues and solutions

### Architecture Documentation
- Keep `README.md` updated with architecture changes
- Document data flow diagrams
- Maintain API documentation
- Version breaking changes in CHANGELOG

## 🚀 Deployment Rules

### Docker Best Practices
1. **Multi-stage builds** - Minimize image size
2. **Non-root users** - Security best practice
3. **Health checks** - All services must have health checks
4. **Resource limits** - Set CPU/memory limits
5. **Volumes** - Persist data appropriately

### Environment Management
```bash
# Development
docker-compose up -d

# Production (example)
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# With specific profile
docker-compose --profile gpu up -d
```

## 🔒 Security Rules

### Credentials Management
1. **Never commit secrets** - Use `.env` files (gitignored)
2. **Environment variables** - For all sensitive data
3. **MinIO credentials** - Change defaults in production
4. **API keys** - Rotate regularly

### Data Privacy
1. **PII Handling** - Mask sensitive data in logs
2. **Data Retention** - Follow policy_documents.md guidelines
3. **Access Control** - Implement authentication for production
4. **Audit Logging** - Log all data access

## 🔄 Version Control Rules

### Git Workflow
```bash
# Feature branches
git checkout -b feature/add-new-llm-provider

# Commit messages
git commit -m "feat: add OpenAI provider support

- Add OpenAIClient class
- Update settings for API key
- Add tests for OpenAI integration
- Update README with OpenAI setup

Closes #123"
```

### Commit Message Format
- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation changes
- `style:` - Code style changes (formatting)
- `refactor:` - Code refactoring
- `test:` - Adding tests
- `chore:` - Maintenance tasks

## 🚫 Prohibited Practices

### Never Do This
1. ❌ Hardcode credentials or API keys
2. ❌ Use `print()` for logging (use `logger`)
3. ❌ Commit `.env` files with real credentials
4. ❌ Skip error handling for "quick tests"
5. ❌ Use global variables (except constants)
6. ❌ Mix business logic with configuration
7. ❌ Create circular dependencies between modules
8. ❌ Use mutable default arguments in functions

### Example: Bad vs Good
```python
# ❌ BAD
def add_item(item, list=[]):
    list.append(item)
    return list

# ✅ GOOD
def add_item(item: Any, items_list: Optional[List] = None) -> List:
    if items_list is None:
        items_list = []
    items_list.append(item)
    return items_list
```

## 🔍 Code Review Checklist

Before committing, verify:
- [ ] Code follows PEP 8 style
- [ ] All functions have type hints
- [ ] All public methods have docstrings
- [ ] No hardcoded credentials
- [ ] Error handling implemented
- [ ] Logging added for key operations
- [ ] Tests written for new functionality
- [ ] README updated if needed
- [ ] No console.log() or print() statements
- [ ] No commented-out code

## 📊 Performance Rules

### Optimization Guidelines
1. **Batch Operations** - Process in batches, not one-by-one
2. **Connection Pooling** - Reuse database connections
3. **Caching** - Cache frequent queries (Redis/Memcached)
4. **Async Operations** - Use async/await for I/O-bound tasks
5. **Resource Cleanup** - Close connections, free memory

### Monitoring Requirements
- Log slow operations (>1s)
- Track memory usage
- Monitor API rate limits
- Alert on error spikes

## 🔮 Future-Proofing

### Extensibility
1. **Plugin Architecture** - Design for extension
2. **Interface Segregation** - Small, focused interfaces
3. **Dependency Injection** - Make dependencies explicit
4. **Configuration-Driven** - Minimize hardcoded logic

### Backward Compatibility
- Version your APIs
- Deprecate features gracefully
- Maintain migration paths
- Document breaking changes

## 📚 Learning Resources

### Internal Documentation
- `README.md` - Project overview and setup
- `CLINE_RULES.md` - This file
- Code comments - Implementation details
- Docstrings - API documentation

### External References
- [Apache Flink Docs](https://flink.apache.org/documentation/)
- [Apache Iceberg Docs](https://iceberg.apache.org/docs/)
- [Chroma Docs](https://docs.trychroma.com/)
- [LangChain Docs](https://python.langchain.com/)
- [Ollama Docs](https://ollama.ai/docs)

## 🎓 Development Workflow

### Adding New Features
1. **Plan** - Document the feature and approach
2. **Branch** - Create feature branch
3. **Implement** - Write code following these rules
4. **Test** - Add tests for new functionality
5. **Document** - Update README and docstrings
6. **Review** - Self-review against checklist
7. **Commit** - Follow commit message format
8. **Merge** - After approval

### Debugging Workflow
1. **Reproduce** - Create minimal reproduction
2. **Log** - Add strategic logging
3. **Isolate** - Identify the component
4. **Fix** - Implement solution
5. **Test** - Verify fix works
6. **Prevent** - Add test to prevent regression

## 🏆 Quality Standards

### Code Quality Metrics
- **Cyclomatic Complexity:** < 10 per function
- **Function Length:** < 50 lines
- **Class Length:** < 300 lines
- **Parameter Count:** < 5 per function
- **Nesting Depth:** < 4 levels

### Definition of Done
A feature is complete when:
- [ ] Code implemented following these rules
- [ ] Unit tests written and passing
- [ ] Integration tests passing
- [ ] Documentation updated
- [ ] Code reviewed
- [ ] No console errors or warnings
- [ ] Performance acceptable

---

## Quick Reference

### Common Commands
```bash
# Setup
./start_pipeline.sh

# Testing
python test_pipeline.py
python test_pipeline.py --test vector-db

# Vector DB
python vector-db/setup_vector_db.py --reset --test

# RAG App
python rag-app/rag_app.py --interactive
python rag-app/rag_app.py --customer-id 123 --query "Why delayed?"

# Docker
docker-compose up -d
docker-compose ps
docker-compose logs -f [service]
```

### Important Paths
```
Project Root: /rag-pipeline
Config: /rag-pipeline/config/settings.py
Logs: Check docker-compose logs
Data: MinIO bucket 'warehouse'
Vector DB: Chroma at localhost:8000
```

---

**Remember:** These rules exist to maintain code quality and make the project maintainable. When in doubt, prioritize clarity and simplicity over cleverness.

**Last Updated:** 2024-07-03
**Version:** 1.0.0