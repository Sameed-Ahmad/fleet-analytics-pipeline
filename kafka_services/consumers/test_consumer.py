"""
Kafka Test Consumer for Fleet Analytics Pipeline
Used to verify messages are flowing through Kafka topics
"""

import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import signal
import sys
from datetime import datetime
from collections import defaultdict

class FleetKafkaTestConsumer:
    """
    Test consumer to verify Kafka message flow
    """
    
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topics: list = None,
        group_id: str = 'fleet-test-consumer'
    ):
        """
        Initialize test consumer
        
        Args:
            bootstrap_servers: Kafka broker address
            topics: List of topics to consume from
            group_id: Consumer group ID
        """
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics or ['vehicle-telemetry', 'deliveries', 'incidents']
        self.group_id = group_id
        self.consumer = None
        self.logger = self._setup_logger()
        self.running = True
        self.stats = defaultdict(int)
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('FleetKafkaTestConsumer')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        self.logger.info(f"\n🛑 Received signal {signum}, shutting down...")
        self.running = False
    
    def connect(self):
        """Establish connection to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                group_id=self.group_id,
                auto_offset_reset='latest',  # Start from latest messages
                enable_auto_commit=True,
                consumer_timeout_ms=1000,
                max_poll_records=10
            )
            self.logger.info(f"✅ Connected to Kafka at {self.bootstrap_servers}")
            self.logger.info(f"📝 Subscribed to topics: {', '.join(self.topics)}")
        
        except KafkaError as e:
            self.logger.error(f"❌ Failed to connect to Kafka: {e}")
            raise
    
    def consume_messages(self, max_messages: int = None, verbose: bool = True):
        """
        Consume messages from Kafka topics
        
        Args:
            max_messages: Maximum number of messages to consume (None = unlimited)
            verbose: Print detailed message information
        """
        if not self.consumer:
            self.connect()
        
        self.logger.info(f"🎧 Starting to consume messages... (Press Ctrl+C to stop)")
        message_count = 0
        
        try:
            while self.running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                if not messages:
                    continue
                
                for topic_partition, records in messages.items():
                    for record in records:
                        message_count += 1
                        self.stats[record.topic] += 1
                        
                        if verbose:
                            self._print_message(record, message_count)
                        else:
                            self._print_summary(record, message_count)
                        
                        # Check if we've reached max messages
                        if max_messages and message_count >= max_messages:
                            self.logger.info(f"✅ Reached max messages limit ({max_messages})")
                            self.running = False
                            break
                    
                    if not self.running:
                        break
        
        except KeyboardInterrupt:
            self.logger.info("\n⏸️  Consumption interrupted by user")
        
        finally:
            self._print_final_stats()
            self.close()
    
    def _print_message(self, record, count: int):
        """Print detailed message information"""
        print(f"\n{'='*80}")
        print(f"📨 Message #{count}")
        print(f"{'='*80}")
        print(f"Topic:     {record.topic}")
        print(f"Partition: {record.partition}")
        print(f"Offset:    {record.offset}")
        print(f"Key:       {record.key}")
        print(f"Timestamp: {datetime.fromtimestamp(record.timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\n📄 Payload:")
        print(json.dumps(record.value, indent=2))
    
    def _print_summary(self, record, count: int):
        """Print brief message summary"""
        timestamp = datetime.fromtimestamp(record.timestamp/1000).strftime('%H:%M:%S')
        print(f"[{timestamp}] #{count} | {record.topic} | Key: {record.key}")
    
    def _print_final_stats(self):
        """Print consumption statistics"""
        print(f"\n{'='*80}")
        print("📊 CONSUMPTION STATISTICS")
        print(f"{'='*80}")
        
        total = sum(self.stats.values())
        print(f"Total messages consumed: {total}")
        print(f"\nBreakdown by topic:")
        for topic, count in sorted(self.stats.items()):
            percentage = (count / total * 100) if total > 0 else 0
            print(f"  • {topic}: {count} ({percentage:.1f}%)")
    
    def peek_topics(self, messages_per_topic: int = 5):
        """
        Peek at latest messages from each topic
        
        Args:
            messages_per_topic: Number of messages to peek from each topic
        """
        self.logger.info(f"👀 Peeking at {messages_per_topic} messages per topic...")
        
        for topic in self.topics:
            print(f"\n{'='*80}")
            print(f"📋 Topic: {topic}")
            print(f"{'='*80}")
            
            # Create consumer for this specific topic
            temp_consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                consumer_timeout_ms=2000,
                max_poll_records=messages_per_topic
            )
            
            messages_read = 0
            for message in temp_consumer:
                messages_read += 1
                print(f"\n  Message {messages_read}:")
                print(f"  Key: {message.key}")
                print(f"  Value: {json.dumps(message.value, indent=4)[:200]}...")
                
                if messages_read >= messages_per_topic:
                    break
            
            if messages_read == 0:
                print("  ⚠️  No messages available (topic might be empty)")
            
            temp_consumer.close()
    
    def close(self):
        """Close consumer connection"""
        if self.consumer:
            self.logger.info("Closing Kafka consumer...")
            self.consumer.close()
            self.logger.info("✅ Kafka consumer closed")


def main():
    """Main function with CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Test Consumer for Fleet Analytics')
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--topics',
        nargs='+',
        default=['vehicle-telemetry', 'deliveries', 'incidents'],
        help='Topics to consume from'
    )
    parser.add_argument(
        '--max-messages',
        type=int,
        default=None,
        help='Maximum number of messages to consume (default: unlimited)'
    )
    parser.add_argument(
        '--mode',
        choices=['consume', 'peek'],
        default='consume',
        help='Mode: consume (continuous) or peek (sample)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print detailed message information'
    )
    
    args = parser.parse_args()
    
    # Create consumer
    consumer = FleetKafkaTestConsumer(
        bootstrap_servers=args.bootstrap_servers,
        topics=args.topics
    )
    
    try:
        if args.mode == 'peek':
            consumer.connect()
            consumer.peek_topics(messages_per_topic=5)
        else:
            consumer.consume_messages(
                max_messages=args.max_messages,
                verbose=args.verbose
            )
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()