"""
PyFlink Streaming Job
Consumes order events from a socket stream and writes to Apache Iceberg table.
"""

import json
import logging
from typing import Dict, Any
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import MapFunction, FilterFunction
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.catalog import Catalog
from pyflink.table.descriptors import Kafka, Json, Schema
from pyflink.table.types import DataTypes
from pyflink.table.expressions import col
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()


class OrderEvent:
    """Represents an order event."""
    
    def __init__(self, order_id: str, customer_id: str, status: str, 
                 delivery_notes: str, timestamp: str, items: list,
                 total_amount: float, shipping_address: str, 
                 carrier: str, tracking_number: str):
        self.order_id = order_id
        self.customer_id = customer_id
        self.status = status
        self.delivery_notes = delivery_notes
        self.timestamp = timestamp
        self.items = items
        self.total_amount = total_amount
        self.shipping_address = shipping_address
        self.carrier = carrier
        self.tracking_number = tracking_number
    
    @staticmethod
    def from_json(json_str: str) -> 'OrderEvent':
        """Create OrderEvent from JSON string."""
        data = json.loads(json_str)
        return OrderEvent(
            order_id=data.get('order_id', ''),
            customer_id=data.get('customer_id', ''),
            status=data.get('status', ''),
            delivery_notes=data.get('delivery_notes', ''),
            timestamp=data.get('timestamp', ''),
            items=data.get('items', []),
            total_amount=float(data.get('total_amount', 0.0)),
            shipping_address=data.get('shipping_address', ''),
            carrier=data.get('carrier', ''),
            tracking_number=data.get('tracking_number', '')
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'order_id': self.order_id,
            'customer_id': self.customer_id,
            'status': self.status,
            'delivery_notes': self.delivery_notes,
            'timestamp': self.timestamp,
            'items': json.dumps(self.items),
            'total_amount': self.total_amount,
            'shipping_address': self.shipping_address,
            'carrier': self.carrier,
            'tracking_number': self.tracking_number
        }


class JsonToOrderEventMapFunction(MapFunction):
    """MapFunction to convert JSON string to OrderEvent."""
    
    def map(self, value: str) -> OrderEvent:
        """Map JSON string to OrderEvent."""
        try:
            return OrderEvent.from_json(value)
        except Exception as e:
            logger.error(f"Error parsing JSON: {value}, error: {e}")
            raise


class OrderEventToJsonMapFunction(MapFunction):
    """MapFunction to convert OrderEvent to JSON string."""
    
    def map(self, order_event: OrderEvent) -> str:
        """Map OrderEvent to JSON string."""
        return json.dumps(order_event.to_dict())


class FlinkIcebergJob:
    """PyFlink job to consume orders and write to Iceberg."""
    
    def __init__(self):
        """Initialize the Flink job."""
        self.settings = get_settings()
        self.env = None
        self.t_env = None
    
    def setup_environment(self):
        """Setup Flink execution environment."""
        logger.info("Setting up Flink execution environment...")
        
        # Create execution environment
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        self.env.set_parallelism(1)
        
        # Enable checkpointing
        self.env.enable_checkpointing(5000)  # Checkpoint every 5 seconds
        
        # Create table environment
        env_settings = EnvironmentSettings.in_streaming_mode()
        self.t_env = StreamTableEnvironment.create(
            self.env,
            environment_settings=env_settings
        )
        
        logger.info("Flink environment setup complete")
    
    def create_iceberg_catalog(self):
        """Create and configure Iceberg catalog."""
        logger.info(f"Creating Iceberg catalog: {self.settings.iceberg_catalog_name}")
        
        # Create catalog
        catalog = self.t_env.create_catalog(self.settings.iceberg_catalog_name)
        
        # Set catalog as current catalog
        self.t_env.use_catalog(self.settings.iceberg_catalog_name)
        
        # Create namespace if not exists
        self.t_env.execute_sql(f"""
            CREATE NAMESPACE IF NOT EXISTS {self.settings.iceberg_namespace}
        """)
        
        # Use namespace
        self.t_env.use_namespace(self.settings.iceberg_namespace)
        
        logger.info(f"Iceberg catalog '{self.settings.iceberg_catalog_name}' configured")
    
    def create_iceberg_table(self):
        """Create Iceberg table for orders."""
        logger.info(f"Creating Iceberg table: {self.settings.iceberg_table_name}")
        
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.settings.iceberg_table_name} (
                order_id STRING,
                customer_id STRING,
                status STRING,
                delivery_notes STRING,
                timestamp STRING,
                items STRING,
                total_amount DOUBLE,
                shipping_address STRING,
                carrier STRING,
                tracking_number STRING,
                ingestion_time TIMESTAMP(3) METADATA FROM 'values.ingestion-time' VIRTUAL
            ) PARTITIONED BY (customer_id)
            WITH (
                'format-version' = '2',
                'write.parquet.compression-codec' = 'snappy'
            )
        """
        
        self.t_env.execute_sql(create_table_sql)
        logger.info(f"Iceberg table '{self.settings.iceberg_table_name}' created")
    
    def create_kafka_source(self):
        """Create Kafka source for order events."""
        logger.info("Setting up socket source for order events...")
        
        # For this prototype, we'll use a socket source instead of Kafka
        # In production, you would use Kafka
        socket_source_ddl = f"""
            CREATE TABLE orders_source (
                order_id STRING,
                customer_id STRING,
                status STRING,
                delivery_notes STRING,
                timestamp STRING,
                items STRING,
                total_amount DOUBLE,
                shipping_address STRING,
                carrier STRING,
                tracking_number STRING
            ) WITH (
                'connector' = 'socket',
                'hostname' = '{self.settings.flink_jobmanager_host}',
                'port' = '9999',
                'format' = 'json',
                'json.fail-on-missing-field' = 'false',
                'json.ignore-parse-errors' = 'true'
            )
        """
        
        self.t_env.execute_sql(socket_source_ddl)
        logger.info("Socket source created")
    
    def insert_into_iceberg(self):
        """Insert data from source to Iceberg table."""
        logger.info("Setting up insert from source to Iceberg...")
        
        insert_sql = f"""
            INSERT INTO {self.settings.iceberg_table_name}
            SELECT 
                order_id,
                customer_id,
                status,
                delivery_notes,
                timestamp,
                items,
                total_amount,
                shipping_address,
                carrier,
                tracking_number
            FROM orders_source
        """
        
        # Execute the insert statement
        statement_set = self.t_env.create_statement_set()
        statement_set.add_insert_sql(insert_sql)
        
        logger.info("Insert statement prepared")
        return statement_set
    
    def run(self):
        """Run the Flink job."""
        logger.info("=" * 80)
        logger.info("Starting PyFlink Order Processing Job")
        logger.info("=" * 80)
        
        try:
            # Setup environment
            self.setup_environment()
            
            # Create Iceberg catalog and table
            self.create_iceberg_catalog()
            self.create_iceberg_table()
            
            # Create source
            self.create_kafka_source()
            
            # Prepare insert statement
            statement_set = self.insert_into_iceberg()
            
            # Execute the job
            logger.info("Executing Flink job...")
            statement_set.execute()
            
            logger.info("Flink job submitted successfully")
            logger.info(f"Orders will be written to Iceberg table: {self.settings.iceberg_table_name}")
            logger.info("Job is now running and waiting for data...")
            
        except Exception as e:
            logger.error(f"Error running Flink job: {e}", exc_info=True)
            raise


def main():
    """Main entry point."""
    job = FlinkIcebergJob()
    job.run()


if __name__ == "__main__":
    main()