#!/usr/bin/env python3
"""
MongoDB Kafka Consumer
Reads from Kafka topics and writes to MongoDB
This bridges the gap until Spark Streaming is configured in Phase 7
"""

import json
import logging
from kafka import KafkaConsumer
from pymongo import MongoClient
from datetime import datetime
import signal
import sys
from collections import defaultdict

class MongoDBKafkaConsumer:
    """
    Consumes messages from Kafka and writes them to MongoDB
    """
    
    def __init__(
        self,
        kafka_servers='localhost:9092',
        mongo_uri='mongodb://admin:admin123@localhost:27017/'
    ):
        """
        Initialize consumer
        
        Args:
            kafka_servers: Kafka bootstrap servers
            mongo_uri: MongoDB connection string
        """
        self.kafka_servers = kafka_servers
        self.mongo_uri = mongo_uri
        self.consumer = None
        self.mongo_client = None
        self.db = None
        self.running = True
        self.stats = defaultdict(int)
        self.logger = self._setup_logger()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger('MongoDBKafkaConsumer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"\n Received signal {signum}, shutting down...")
        self.running = False
    
    def connect(self):
        """Connect to Kafka and MongoDB"""
        # Connect to MongoDB
        self.logger.info("Connecting to MongoDB...")
        self.mongo_client = MongoClient(self.mongo_uri)
        self.db = self.mongo_client['fleet_analytics']
        self.logger.info("Connected to MongoDB")
        
        # Connect to Kafka
        self.logger.info("Connecting to Kafka...")
        self.consumer = KafkaConsumer(
            'vehicle-telemetry',
            'deliveries',
            'incidents',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',  # Start from beginning
            enable_auto_commit=True,
            group_id='mongodb-writer',
            consumer_timeout_ms=1000
        )
        self.logger.info("Connected to Kafka")
        self.logger.info("Subscribed to: vehicle-telemetry, deliveries, incidents")
    
    def _parse_timestamp(self, timestamp_str):
        """Parse timestamp string to datetime"""
        try:
            # Remove 'Z' if present and parse
            if isinstance(timestamp_str, str):
                timestamp_str = timestamp_str.replace('Z', '')
                return datetime.fromisoformat(timestamp_str)
            return timestamp_str
        except Exception as e:
            self.logger.warning(f"Failed to parse timestamp: {e}")
            return datetime.utcnow()
    
    def _write_telemetry(self, data):
        """Write telemetry event to MongoDB"""
        try:
            # Parse timestamp
            data['timestamp'] = self._parse_timestamp(data.get('timestamp'))
            
            # Insert into telemetry_events
            self.db.telemetry_events.insert_one(data)
            self.stats['telemetry'] += 1
            
        except Exception as e:
            self.logger.error(f"Error writing telemetry: {e}")
            self.stats['errors'] += 1
    
    def _write_delivery(self, data):
        """Write delivery event to MongoDB"""
        try:
            # Parse timestamps
            if 'pickup_time' in data:
                data['pickup_time'] = self._parse_timestamp(data['pickup_time'])
            if 'estimated_delivery' in data:
                data['estimated_delivery'] = self._parse_timestamp(data['estimated_delivery'])
            if 'completion_time' in data:
                data['completion_time'] = self._parse_timestamp(data['completion_time'])
            
            # Insert or update delivery
            self.db.deliveries.update_one(
                {'delivery_id': data['delivery_id']},
                {'$set': data},
                upsert=True
            )
            self.stats['deliveries'] += 1
            
        except Exception as e:
            self.logger.error(f"Error writing delivery: {e}")
            self.stats['errors'] += 1
    
    def _write_incident(self, data):
        """Write incident event to MongoDB"""
        try:
            # Parse timestamp
            data['timestamp'] = self._parse_timestamp(data.get('timestamp'))
            
            # Insert incident
            self.db.incidents.insert_one(data)
            self.stats['incidents'] += 1
            
        except Exception as e:
            self.logger.error(f"Error writing incident: {e}")
            self.stats['errors'] += 1
    
    def consume_and_write(self):
        """Main consumption loop"""
        self.logger.info("Starting to consume and write to MongoDB...")
        self.logger.info("   Press Ctrl+C to stop\n")
        
        message_count = 0
        
        try:
            while self.running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    topic = topic_partition.topic
                    
                    for record in records:
                        message_count += 1
                        
                        try:
                            # Route to appropriate writer
                            if topic == 'vehicle-telemetry':
                                self._write_telemetry(record.value)
                            elif topic == 'deliveries':
                                self._write_delivery(record.value)
                            elif topic == 'incidents':
                                self._write_incident(record.value)
                            
                            # Print progress every 100 messages
                            if message_count % 100 == 0:
                                self.logger.info(
                                    f"Processed {message_count} messages | "
                                    f"Telemetry: {self.stats['telemetry']}, "
                                    f"Deliveries: {self.stats['deliveries']}, "
                                    f"Incidents: {self.stats['incidents']}, "
                                    f"Errors: {self.stats['errors']}"
                                )
                        
                        except Exception as e:
                            self.logger.error(f"Error processing message: {e}")
                            self.stats['errors'] += 1
        
        except KeyboardInterrupt:
            self.logger.info("\n Consumption interrupted by user")
        
        finally:
            self._print_final_stats()
            self.close()
    
    def _print_final_stats(self):
        """Print final statistics"""
        print(f"\n{'='*80}")
        print("FINAL STATISTICS")
        print(f"{'='*80}")
        print(f"Telemetry events written: {self.stats['telemetry']:,}")
        print(f"Delivery events written:  {self.stats['deliveries']:,}")
        print(f"Incident events written:  {self.stats['incidents']:,}")
        print(f"Errors:                   {self.stats['errors']:,}")
        print(f"Total processed:          {sum(v for k, v in self.stats.items() if k != 'errors'):,}")
        print(f"{'='*80}")
    
    def close(self):
        """Close connections"""
        if self.consumer:
            self.logger.info("Closing Kafka consumer...")
            self.consumer.close()
        
        if self.mongo_client:
            self.logger.info("Closing MongoDB connection...")
            self.mongo_client.close()
        
        self.logger.info("Shutdown complete")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='MongoDB Kafka Consumer - Writes Kafka messages to MongoDB'
    )
    parser.add_argument(
        '--kafka-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers'
    )
    parser.add_argument(
        '--mongo-uri',
        default='mongodb://admin:admin123@localhost:27017/',
        help='MongoDB connection URI'
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("MongoDB Kafka Consumer")
    print("Reads from Kafka → Writes to MongoDB")
    print("=" * 80)
    print()
    
    consumer = MongoDBKafkaConsumer(
        kafka_servers=args.kafka_servers,
        mongo_uri=args.mongo_uri
    )
    
    try:
        consumer.connect()
        consumer.consume_and_write()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()