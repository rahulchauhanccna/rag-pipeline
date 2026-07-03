"""
Mock Order Stream Generator
Generates continuous stream of e-commerce order updates as JSON events.
Simulates real-time order status changes including a specific delayed order for customer_id: 123.
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any
from faker import Faker
import socket
import sys

# Initialize Faker for realistic data generation
fake = Faker()


class OrderStreamGenerator:
    """Generates mock e-commerce order events."""
    
    def __init__(self, customer_123_delayed: bool = True):
        """
        Initialize the order stream generator.
        
        Args:
            customer_123_delayed: If True, customer_id 123 will have a delayed order
        """
        self.customer_123_delayed = customer_123_delayed
        self.orders_db: Dict[str, Dict[str, Any]] = {}
        self.statuses = [
            "pending", "confirmed", "processing", "shipped", 
            "in_transit", "out_for_delivery", "delivered", "cancelled"
        ]
        
        # Pre-create customer 123's delayed order
        if customer_123_delayed:
            self._create_customer_123_order()
    
    def _create_customer_123_order(self):
        """Create a specific delayed order for customer_id 123."""
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        self.orders_db[order_id] = {
            "order_id": order_id,
            "customer_id": "123",
            "status": "in_transit",
            "delivery_notes": "Package delayed due to weather conditions. Expected delivery: 3-5 business days.",
            "timestamp": datetime.now().isoformat(),
            "items": [
                {"product": "Wireless Headphones", "quantity": 1, "price": 79.99},
                {"product": "USB-C Cable", "quantity": 2, "price": 12.99}
            ],
            "total_amount": 105.97,
            "shipping_address": fake.address(),
            "carrier": "FedEx",
            "tracking_number": f"FDX{uuid.uuid4().hex[:12].upper()}"
        }
    
    def generate_order_event(self) -> Dict[str, Any]:
        """
        Generate a random order event.
        
        Returns:
            Dictionary containing order event data
        """
        # 20% chance to update customer 123's order (if it exists)
        if self.customer_123_delayed and random.random() < 0.2:
            customer_123_orders = [oid for oid, order in self.orders_db.items() 
                                   if order["customer_id"] == "123"]
            if customer_123_orders:
                order_id = random.choice(customer_123_orders)
                order = self.orders_db[order_id]
                
                # Update status with progression
                current_status_idx = self.statuses.index(order["status"])
                if current_status_idx < len(self.statuses) - 1:
                    # 30% chance to progress, 70% stay delayed
                    if random.random() < 0.3:
                        order["status"] = self.statuses[current_status_idx + 1]
                        order["delivery_notes"] = f"Order status updated to {order['status']}"
                    else:
                        order["delivery_notes"] = "Still delayed. We apologize for the inconvenience."
                order["timestamp"] = datetime.now().isoformat()
                return order.copy()
        
        # Generate new random order
        order_id = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        customer_id = str(random.randint(1, 1000))
        status = random.choice(self.statuses)
        
        # Generate realistic delivery notes based on status
        if status == "delivered":
            delivery_notes = random.choice([
                "Package delivered successfully.",
                "Left at front door as requested.",
                "Delivered to neighbor.",
                "Signed by recipient."
            ])
        elif status in ["shipped", "in_transit", "out_for_delivery"]:
            delivery_notes = random.choice([
                "Package is on its way.",
                "In transit to destination.",
                "Out for delivery today.",
                "Expected delivery tomorrow."
            ])
        elif status == "cancelled":
            delivery_notes = "Order cancelled by customer."
        else:
            delivery_notes = "Order processing."
        
        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "status": status,
            "delivery_notes": delivery_notes,
            "timestamp": datetime.now().isoformat(),
            "items": [
                {
                    "product": fake.word().title(),
                    "quantity": random.randint(1, 5),
                    "price": round(random.uniform(10.0, 200.0), 2)
                }
                for _ in range(random.randint(1, 4))
            ],
            "total_amount": round(random.uniform(20.0, 500.0), 2),
            "shipping_address": fake.address(),
            "carrier": random.choice(["UPS", "FedEx", "USPS", "DHL"]),
            "tracking_number": f"{random.choice(['UPS','FDX','USP','DHL'])}{uuid.uuid4().hex[:12].upper()}"
        }
        
        self.orders_db[order_id] = order
        return order.copy()
    
    def stream_to_socket(self, host: str = "localhost", port: int = 9999, 
                        delay: float = 1.0, num_events: int = None):
        """
        Stream order events to a TCP socket.
        
        Args:
            host: Socket host
            port: Socket port
            delay: Delay between events in seconds
            num_events: Number of events to generate (None for infinite)
        """
        print(f"Starting order stream generator on {host}:{port}")
        print(f"Customer 123 has a delayed order: {self.customer_123_delayed}")
        print("Press Ctrl+C to stop\n")
        
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((host, port))
        server_socket.listen(5)
        
        print(f"Waiting for Flink to connect on {host}:{port}...")
        client_socket, addr = server_socket.accept()
        print(f"Connected to {addr}")
        
        count = 0
        try:
            while num_events is None or count < num_events:
                event = self.generate_order_event()
                event_json = json.dumps(event) + "\n"
                
                try:
                    client_socket.sendall(event_json.encode('utf-8'))
                    print(f"[{count+1}] Sent: {event['order_id']} - Customer {event['customer_id']} - {event['status']}")
                    count += 1
                except (BrokenPipeError, ConnectionResetError):
                    print("Client disconnected. Waiting for new connection...")
                    client_socket, addr = server_socket.accept()
                    print(f"Reconnected to {addr}")
                    continue
                
                time.sleep(delay)
                
        except KeyboardInterrupt:
            print("\nStopping stream generator...")
        finally:
            client_socket.close()
            server_socket.close()
            print(f"Generated {count} order events")
    
    def stream_to_console(self, delay: float = 1.0, num_events: int = 20):
        """
        Stream order events to console (for testing).
        
        Args:
            delay: Delay between events in seconds
            num_events: Number of events to generate
        """
        print("=" * 80)
        print("ORDER STREAM GENERATOR - Console Mode")
        print("=" * 80)
        print(f"Customer 123 has a delayed order: {self.customer_123_delayed}")
        print("=" * 80)
        print()
        
        for i in range(num_events):
            event = self.generate_order_event()
            print(f"[{i+1:3d}] {json.dumps(event, indent=2)}")
            print("-" * 80)
            time.sleep(delay)
        
        print(f"\nGenerated {num_events} order events")


def main():
    """Main entry point for the order stream generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Mock Order Stream Generator")
    parser.add_argument("--mode", choices=["socket", "console"], default="console",
                       help="Stream mode: 'socket' for TCP socket, 'console' for console output")
    parser.add_argument("--host", default="localhost", help="Socket host (socket mode only)")
    parser.add_argument("--port", type=int, default=9999, help="Socket port (socket mode only)")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between events in seconds")
    parser.add_argument("--num-events", type=int, default=None, 
                       help="Number of events to generate (None for infinite in socket mode)")
    parser.add_argument("--no-delayed-order", action="store_true",
                       help="Disable the delayed order for customer 123")
    
    args = parser.parse_args()
    
    generator = OrderStreamGenerator(customer_123_delayed=not args.no_delayed_order)
    
    if args.mode == "socket":
        generator.stream_to_socket(
            host=args.host, 
            port=args.port, 
            delay=args.delay, 
            num_events=args.num_events
        )
    else:
        generator.stream_to_console(delay=args.delay, num_events=args.num_events or 20)


if __name__ == "__main__":
    main()