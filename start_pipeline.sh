#!/bin/bash
# Quick Start Script for Real-Time Customer Support RAG Pipeline

set -e

echo "=========================================="
echo "RAG Pipeline - Quick Start"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed. Please install it and try again."
    exit 1
fi

echo "Step 1: Starting infrastructure services..."
echo "-------------------------------------------"
docker-compose up -d

echo ""
echo "Waiting for services to be ready..."
sleep 10

# Check if services are running
echo ""
echo "Checking service health..."
echo "-------------------------------------------"

# Check MinIO
if curl -s http://localhost:9000/minio/health/live > /dev/null; then
    echo "✓ MinIO is running"
else
    echo "✗ MinIO is not ready yet"
fi

# Check Iceberg REST
if curl -s http://localhost:8181/v1/config > /dev/null; then
    echo "✓ Iceberg REST Catalog is running"
else
    echo "✗ Iceberg REST Catalog is not ready yet"
fi

# Check Flink
if curl -s http://localhost:8081/overview > /dev/null; then
    echo "✓ Flink JobManager is running"
else
    echo "✗ Flink JobManager is not ready yet"
fi

# Check Chroma
if curl -s http://localhost:8000/api/v1/heartbeat > /dev/null; then
    echo "✓ Chroma Vector DB is running"
else
    echo "✗ Chroma Vector DB is not ready yet"
fi

echo ""
echo "Step 2: Setting up Python environment..."
echo "-------------------------------------------"

# First check if system Python already has dependencies
echo "Checking system Python dependencies..."
if /usr/bin/python3 -c "import chromadb, sentence_transformers, langchain, pyiceberg" 2>/dev/null; then
    echo "✓ Core dependencies already installed in system Python"
    PYTHON_CMD="/usr/bin/python3"
    PIP_CMD="pip3"
else
    echo "System Python missing dependencies."
    # Check if venv exists and has packages
    if [ -d "venv" ]; then
        echo "Checking virtual environment..."
        if venv/bin/python3 -c "import chromadb, sentence_transformers, langchain, pyiceberg" 2>/dev/null; then
            echo "✓ Core dependencies found in venv"
            source venv/bin/activate
            PYTHON_CMD="python3"
            PIP_CMD="pip"
        else
            echo "venv exists but missing packages. Installing..."
            source venv/bin/activate
            pip install -q -r requirements.txt
            PYTHON_CMD="python3"
            PIP_CMD="pip"
        fi
    else
        echo "Installing dependencies in system Python (fastest option)..."
        pip3 install -q -r requirements.txt
        PYTHON_CMD="/usr/bin/python3"
        PIP_CMD="pip3"
    fi
fi

echo ""
echo "Step 3: Creating MinIO bucket..."
echo "-------------------------------------------"
echo "Creating 'warehouse' bucket in MinIO..."
docker exec minio mc alias set local http://localhost:9000 admin password123 > /dev/null 2>&1
docker exec minio mc mb local/warehouse > /dev/null 2>&1 && echo "✓ Bucket 'warehouse' created" || echo "✓ Bucket 'warehouse' already exists"

echo ""
echo "Step 4: Setting up Vector Database..."
echo "-------------------------------------------"
$PYTHON_CMD vector_db/setup_vector_db.py --reset

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Start the order stream generator:"
echo "   python data_generators/order_stream_generator.py --mode socket"
echo ""
echo "2. In another terminal, start the Flink job:"
echo "   python flink-job/flink_job.py"
echo ""
echo "3. In another terminal, run the RAG application:"
echo "   python rag_app/rag_app.py --interactive"
echo ""
echo "Or run everything in demo mode:"
echo "   python rag_app/rag_app.py --interactive  # Uses mock data"
echo ""
echo "Service URLs:"
echo "  - MinIO Console: http://localhost:9001"
echo "  - Flink Dashboard: http://localhost:8081"
echo "  - Chroma: http://localhost:8000"
echo ""