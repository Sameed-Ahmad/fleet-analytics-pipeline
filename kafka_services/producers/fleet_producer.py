"""
Kafka Producer for Fleet Analytics Pipeline
Handles publishing messages to Kafka topics with error handling and retries
"""

import json
import logging
from typing import Dict, Any, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

class FleetKafkaProducer:
    """
    Robust Kafka producer with error handling, retries, and logging
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        max_retries: int = 3,
        retry_backoff_ms: int = 1000
    ):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: Kafka broker address
            max_retries: Maximum number of retry attempts
            retry_backoff_ms: Backoff time between retries in milliseconds
        """
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.producer = None
        self.logger = self._setup_logger()
        
        # Topic names
        self.topics = {
            'telemetry': 'vehicle-telemetry',
            'deliveries': 'deliveries',
            'incidents': 'incidents'
        }
        
        self._connect()
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('FleetKafkaProducer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _connect(self):
        """Establish connection to Kafka with retries"""
        for attempt in range(self.max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',  # Wait for all replicas to acknowledge
                    retries=3,
                    max_in_flight_requests_per_connection=5,
                    compression_type='gzip',
                    linger_ms=10,  # Batch messages for efficiency
                    batch_size=16384,
                    buffer_memory=33554432,
                    request_timeout_ms=30000
                )
                self.logger.info(f"✅ Connected to Kafka at {self.bootstrap_servers}")
                return
            
            except KafkaError as e:
                self.logger.error(
                    f"❌ Kafka connection attempt {attempt + 1}/{self.max_retries} failed: {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_backoff_ms / 1000)
                else:
                    raise Exception(f"Failed to connect to Kafka after {self.max_retries} attempts")
    
    def send_telemetry(self, telemetry_data: Dict[str, Any]) -> bool:
        """
        Send vehicle telemetry data to Kafka
        
        Args:
            telemetry_data: Dictionary containing telemetry information
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self._send_message(
            topic=self.topics['telemetry'],
            key=str(telemetry_data.get('vehicle_id', '')),
            value=telemetry_data,
            message_type='telemetry'
        )
    
    def send_delivery(self, delivery_data: Dict[str, Any]) -> bool:
        """
        Send delivery data to Kafka
        
        Args:
            delivery_data: Dictionary containing delivery information
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self._send_message(
            topic=self.topics['deliveries'],
            key=str(delivery_data.get('delivery_id', '')),
            value=delivery_data,
            message_type='delivery'
        )
    
    def send_incident(self, incident_data: Dict[str, Any]) -> bool:
        """
        Send incident data to Kafka
        
        Args:
            incident_data: Dictionary containing incident information
            
        Returns:
            bool: True if successful, False otherwise
        """
        return self._send_message(
            topic=self.topics['incidents'],
            key=str(incident_data.get('incident_id', '')),
            value=incident_data,
            message_type='incident'
        )
    
    def _send_message(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any],
        message_type: str
    ) -> bool:
        """
        Internal method to send message with error handling
        
        Args:
            topic: Kafka topic name
            key: Message key for partitioning
            value: Message payload
            message_type: Type of message for logging
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.producer:
            self.logger.error("Producer not initialized")
            return False
        
        for attempt in range(self.max_retries):
            try:
                # Send message asynchronously
                future = self.producer.send(
                    topic=topic,
                    key=key,
                    value=value
                )
                
                # Wait for confirmation (with timeout)
                record_metadata = future.get(timeout=10)
                
                self.logger.debug(
                    f"✅ {message_type.capitalize()} sent to {topic} "
                    f"[partition: {record_metadata.partition}, "
                    f"offset: {record_metadata.offset}]"
                )
                return True
            
            except KafkaError as e:
                self.logger.error(
                    f"❌ Failed to send {message_type} (attempt {attempt + 1}/{self.max_retries}): {e}"
                )
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_backoff_ms / 1000)
                else:
                    self.logger.error(f"Failed to send {message_type} after {self.max_retries} attempts")
                    return False
            
            except Exception as e:
                self.logger.error(f"Unexpected error sending {message_type}: {e}")
                return False
        
        return False
    
    def flush(self):
        """Flush any pending messages"""
        if self.producer:
            self.producer.flush()
            self.logger.info("📤 Flushed pending messages")
    
    def close(self):
        """Close producer connection gracefully"""
        if self.producer:
            self.logger.info("Closing Kafka producer...")
            self.producer.flush()
            self.producer.close()
            self.logger.info("✅ Kafka producer closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics"""
        if self.producer:
            metrics = self.producer.metrics()
            return {
                'connected': True,
                'topics': list(self.topics.values()),
                'metrics': {
                    'record_send_total': metrics.get('record-send-total', {}).get('value', 0),
                    'record_error_total': metrics.get('record-error-total', {}).get('value', 0)
                }
            }
        return {'connected': False}


# Example usage
if __name__ == "__main__":
    # Test the producer
    producer = FleetKafkaProducer()
    
    # Test telemetry message
    test_telemetry = {
        'vehicle_id': 'VEH001',
        'timestamp': '2025-12-14T10:30:00Z',
        'latitude': 24.8607,
        'longitude': 67.0011,
        'speed': 45.5,
        'fuel_level': 75.2,
        'engine_temp': 85.0
    }
    
    success = producer.send_telemetry(test_telemetry)
    print(f"Telemetry sent: {success}")
    
    # Get stats
    stats = producer.get_stats()
    print(f"Producer stats: {stats}")
    
    # Close connection
    producer.close()