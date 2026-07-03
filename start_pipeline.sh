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

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found."
    echo "Options:"
    echo "  1) Create virtual environment (recommended, takes ~30s)"
    echo "  2) Use system Python (faster, may affect other projects)"
    read -p "Choose (1/2, default: 2): " venv_choice
    venv_choice=${venv_choice:-2}
    
    if [ "$venv_choice" = "1" ]; then
        echo "Creating virtual environment..."
        python3 -m venv venv
        source venv/bin/activate
    else
        echo "Using system Python..."
    fi
else
    echo "Activating existing virtual environment..."
    source venv/bin/activate
fi

# Check if dependencies are already installed
echo "Checking Python dependencies..."
if python3 -c "import chromadb, sentence_transformers, langchain, pyiceberg" 2>/dev/null; then
    echo "✓ Core dependencies already installed"
else
    echo "Installing Python dependencies..."
    pip install -q -r requirements.txt
fi

echo ""
echo "Step 3: Creating MinIO bucket..."
echo "-------------------------------------------"
echo "Please create the 'warehouse' bucket manually:"
echo "1. Open http://localhost:9001"
echo "2. Login with: admin / password123"
echo "3. Click 'Create Bucket' and name it: warehouse"
echo ""
read -p "Press Enter once you've created the bucket..."

echo ""
echo "Step 4: Setting up Vector Database..."
echo "-------------------------------------------"
python vector-db/setup_vector_db.py --reset

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Start the order stream generator:"
echo "   python data-generators/order_stream_generator.py --mode socket"
echo ""
echo "2. In another terminal, start the Flink job:"
echo "   python flink-job/flink_job.py"
echo ""
echo "3. In another terminal, run the RAG application:"
echo "   python rag-app/rag_app.py --interactive"
echo ""
echo "Or run everything in demo mode:"
echo "   python rag-app/rag_app.py --interactive  # Uses mock data"
echo ""
echo "Service URLs:"
echo "  - MinIO Console: http://localhost:9001"
echo "  - Flink Dashboard: http://localhost:8081"
echo "  - Chroma: http://localhost:8000"
echo ""