# AWS IoT Core MQTT High Volume Data Streaming Script
# Optimized for 100K records streaming over 5 hours

import json
import time
import ssl
import threading
from datetime import datetime, timedelta
import paho.mqtt.client as mqtt
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

# Configure logging with more detailed output for high volume
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aws_iot_streaming.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# AWS Credentials
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_REGION = "ap-south-1"

class AWSIoTHighVolumeConfig:
    def __init__(self):
        # File paths - Updated for 100K records
        self.DATA_FILE = r"C:\Users\hariprasath.s\Documents\InternalProject\Hackathon\hackathon\telemetry_data.json"  # Your 100K records file
        self.CONNECTIONS_FILE = r"E:\Data\IOT_Telematics\aws_certificates\aws_device_connections.json"
        self.CERT_DIR = r"C:\Users\hariprasath.s\Documents\InternalProject\Hackathon\hackathon\certificates"
        
        # High Volume Streaming settings - Optimized for 5 hours
        self.TOTAL_RECORDS = 100000  # Expected total records
        self.PIPELINE_DURATION_HOURS = 5  # 5 hours pipeline
        self.PIPELINE_DURATION_MINUTES = self.PIPELINE_DURATION_HOURS * 60  # 300 minutes
        
        # Calculate optimal streaming rate
        self.RECORDS_PER_MINUTE = int(self.TOTAL_RECORDS / self.PIPELINE_DURATION_MINUTES) + 5  # 333 + buffer = 338
        self.DELAY_BETWEEN_RECORDS = 60 / self.RECORDS_PER_MINUTE  # ~0.18 seconds between records
        
        # Batch processing for efficiency
        self.BATCH_SIZE = 10  # Process 10 records at once
        self.MAX_THREADS = 20  # Concurrent connections
        
        # MQTT settings optimized for high volume
        self.MQTT_PORT = 8883
        self.MQTT_KEEPALIVE = 120  # Increased for stability
        self.MQTT_QOS = 1  # Ensure delivery
        
        # Progress reporting
        self.PROGRESS_REPORT_INTERVAL = 1000  # Report every 1000 records
        
        print(f"ðŸš€ AWS IoT Core High Volume Streaming Configuration:")
        print(f"- Expected total records: {self.TOTAL_RECORDS:,}")
        print(f"- Pipeline duration: {self.PIPELINE_DURATION_HOURS} hours ({self.PIPELINE_DURATION_MINUTES} minutes)")
        print(f"- Records per minute: {self.RECORDS_PER_MINUTE}")
        print(f"- Delay between records: {self.DELAY_BETWEEN_RECORDS:.3f} seconds")
        print(f"- Batch size: {self.BATCH_SIZE}")
        print(f"- Max concurrent threads: {self.MAX_THREADS}")
        print(f"- Data file: {self.DATA_FILE}")

config = AWSIoTHighVolumeConfig()

class AWSIoTMQTTClient:
    def __init__(self, device_id, certificate_files, iot_endpoint):
        self.device_id = device_id
        self.certificate_files = certificate_files
        self.iot_endpoint = iot_endpoint
        self.client = None
        self.is_connected = False
        self.topic = f"telematics/{device_id}/data"
        self.messages_sent = 0
        self.last_activity = time.time()
        
    def on_connect(self, client, userdata, flags, rc):
        """MQTT connection callback"""
        if rc == 0:
            self.is_connected = True
            self.last_activity = time.time()
            logger.debug(f"âœ… Device {self.device_id} connected to AWS IoT Core")
        else:
            self.is_connected = False
            logger.error(f"âŒ Device {self.device_id} failed to connect. Code: {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """MQTT disconnection callback"""
        self.is_connected = False
        if rc != 0:
            logger.warning(f"âš ï¸ Device {self.device_id} disconnected unexpectedly")
    
    def on_publish(self, client, userdata, mid):
        """MQTT publish callback"""
        self.messages_sent += 1
        self.last_activity = time.time()
        logger.debug(f"ðŸ“¤ Message {mid} published for device {self.device_id}")
    
    def connect(self):
        """Connect device to AWS IoT Core using MQTT with certificates"""
        try:
            # Verify certificate files exist
            cert_file = self.certificate_files.get('certificate')
            key_file = self.certificate_files.get('private_key')
            ca_file = self.certificate_files.get('root_ca')
            
            if not all([cert_file, key_file, ca_file]):
                logger.error(f"âŒ Missing certificate files for {self.device_id}")
                return False
            
            for file_path in [cert_file, key_file, ca_file]:
                if not os.path.exists(file_path):
                    logger.error(f"âŒ Certificate file not found: {file_path}")
                    return False
            
            # Create MQTT client with optimized settings
            self.client = mqtt.Client(
                client_id=f"{self.device_id}_{int(time.time())}", 
                protocol=mqtt.MQTTv311,
                clean_session=True
            )
            
            # Set callbacks
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_publish = self.on_publish
            
            # Configure SSL/TLS with certificates
            self.client.tls_set(
                ca_certs=ca_file,
                certfile=cert_file,
                keyfile=key_file,
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLSv1_2,
                ciphers=None
            )
            
            # Optimized settings for high volume
            self.client.max_inflight_messages_set(100)  # Allow more in-flight messages
            self.client.max_queued_messages_set(1000)   # Increase queue size
            
            # Connect to AWS IoT Core
            logger.debug(f"Connecting device {self.device_id} to AWS IoT Core...")
            
            self.client.connect(self.iot_endpoint, config.MQTT_PORT, config.MQTT_KEEPALIVE)
            self.client.loop_start()
            
            # Wait for connection with timeout
            timeout = 10
            start_time = time.time()
            while not self.is_connected and (time.time() - start_time) < timeout:
                time.sleep(0.1)
            
            return self.is_connected
            
        except Exception as e:
            logger.error(f"âŒ Failed to connect device {self.device_id}: {str(e)}")
            return False
    
    def send_telemetry_batch(self, telemetry_records):
        """Send multiple telemetry records efficiently"""
        try:
            if not self.is_connected:
                return 0
            
            sent_count = 0
            for record in telemetry_records:
                # Convert data to JSON string
                payload = json.dumps(record)
                
                # Publish message to AWS IoT Core topic
                result = self.client.publish(self.topic, payload, qos=config.MQTT_QOS)
                
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    sent_count += 1
                    self.last_activity = time.time()
                else:
                    logger.warning(f"âŒ Failed to send record from {self.device_id}")
                
                # Small delay between messages in batch
                time.sleep(0.01)
            
            return sent_count
                
        except Exception as e:
            logger.error(f"âŒ Error sending telemetry batch from {self.device_id}: {str(e)}")
            return 0
    
    def is_healthy(self):
        """Check if connection is healthy"""
        return self.is_connected and (time.time() - self.last_activity) < 300  # 5 minutes timeout
    
    def disconnect(self):
        """Disconnect from AWS IoT Core"""
        try:
            if self.client:
                self.client.loop_stop()
                self.client.disconnect()
            logger.debug(f"Device {self.device_id} disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting {self.device_id}: {str(e)}")

class AWSIoTHighVolumeStreamer:
    def __init__(self):
        self.device_clients = {}
        self.telemetry_data = []
        self.device_connections = {}
        self.iot_endpoint = None
        
        # Enhanced statistics for high volume
        self.streaming_stats = {
            'total_records': 0,
            'sent_records': 0,
            'failed_records': 0,
            'start_time': None,
            'devices_connected': 0,
            'batches_processed': 0,
            'records_per_second': 0,
            'estimated_completion': None
        }
        
        # Thread management
        self.executor = None
        self.record_queue = queue.Queue()
    
    def load_device_connections(self):
        """Load device connection information from AWS provisioning results"""
        try:
            if not os.path.exists(config.CONNECTIONS_FILE):
                logger.error(f"âŒ Device connections file not found: {config.CONNECTIONS_FILE}")
                return False
            
            with open(config.CONNECTIONS_FILE, 'r') as f:
                connections_data = json.load(f)
            
            self.iot_endpoint = connections_data.get('endpoint')
            if not self.iot_endpoint:
                logger.error("âŒ IoT endpoint not found in connections file")
                return False
            
            self.device_connections = connections_data.get('devices', {})
            
            logger.info(f"âœ… Connection info loaded:")
            logger.info(f"   ðŸŒ IoT Endpoint: {self.iot_endpoint}")
            logger.info(f"   ðŸ“± Devices available: {len(self.device_connections)}")
            
            return len(self.device_connections) > 0
            
        except Exception as e:
            logger.error(f"âŒ Failed to load device connections: {str(e)}")
            return False
    
    def load_telemetry_data(self):
        """Load high volume telemetry data from JSON file"""
        try:
            logger.info(f"ðŸ“‚ Loading telemetry data from: {config.DATA_FILE}")
            
            with open(config.DATA_FILE, 'r') as f:
                self.telemetry_data = json.load(f)
            
            self.streaming_stats['total_records'] = len(self.telemetry_data)
            
            logger.info(f"âœ… Loaded {len(self.telemetry_data):,} telemetry records")
            
            # Data analysis
            if self.telemetry_data:
                data_devices = set(record['device_id'] for record in self.telemetry_data)
                available_devices = set(self.device_connections.keys())
                matching_devices = data_devices.intersection(available_devices)
                
                logger.info(f"ðŸ“Š Data Analysis:")
                logger.info(f"   Unique devices in data: {len(data_devices)}")
                logger.info(f"   Devices provisioned: {len(available_devices)}")
                logger.info(f"   Matching devices: {len(matching_devices)}")
                
                # Calculate estimated duration
                estimated_minutes = len(self.telemetry_data) / config.RECORDS_PER_MINUTE
                estimated_hours = estimated_minutes / 60
                completion_time = datetime.now() + timedelta(minutes=estimated_minutes)
                
                logger.info(f"â±ï¸ Streaming Estimates:")
                logger.info(f"   Duration: {estimated_hours:.1f} hours ({estimated_minutes:.0f} minutes)")
                logger.info(f"   Completion: {completion_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                self.streaming_stats['estimated_completion'] = completion_time
            
            return True
            
        except FileNotFoundError:
            logger.error(f"âŒ Telemetry data file not found: {config.DATA_FILE}")
            return False
        except Exception as e:
            logger.error(f"âŒ Failed to load telemetry data: {str(e)}")
            return False
    
    def connect_devices(self):
        """Connect devices to AWS IoT Core with optimized connection pooling"""
        logger.info("ðŸ”Œ Connecting devices to AWS IoT Core...")
        
        # Get devices that have both data and certificates
        data_devices = set(record['device_id'] for record in self.telemetry_data)
        available_devices = set(self.device_connections.keys())
        devices_to_connect = data_devices.intersection(available_devices)
        
        logger.info(f"ðŸ“± Devices to connect: {len(devices_to_connect)}")
        
        # Connect devices with threading for faster connection
        def connect_device(device_id):
            try:
                device_info = self.device_connections[device_id]
                cert_files = device_info.get('certificate_files', {})
                
                client = AWSIoTMQTTClient(device_id, cert_files, self.iot_endpoint)
                
                if client.connect():
                    return device_id, client
                else:
                    return device_id, None
            except Exception as e:
                logger.error(f"âŒ Error connecting {device_id}: {str(e)}")
                return device_id, None
        
        # Use thread pool for parallel connections
        connected_count = 0
        with ThreadPoolExecutor(max_workers=min(10, len(devices_to_connect))) as executor:
            future_to_device = {
                executor.submit(connect_device, device_id): device_id 
                for device_id in devices_to_connect
            }
            
            for future in as_completed(future_to_device):
                device_id, client = future.result()
                if client:
                    self.device_clients[device_id] = client
                    connected_count += 1
                    if connected_count % 50 == 0:  # Progress update
                        logger.info(f"âœ… Connected {connected_count}/{len(devices_to_connect)} devices")
        
        self.streaming_stats['devices_connected'] = connected_count
        
        logger.info(f"ðŸ“Š Connection Summary:")
        logger.info(f"   âœ… Connected: {connected_count}/{len(devices_to_connect)}")
        logger.info(f"   ðŸ“ˆ Success rate: {connected_count/len(devices_to_connect)*100:.1f}%")
        
        return connected_count > 0
    
    def stream_data_high_volume(self):
        """Optimized data streaming for high volume (100K records)"""
        if not self.telemetry_data:
            logger.error("âŒ No telemetry data loaded")
            return
        
        if not self.device_clients:
            logger.error("âŒ No devices connected")
            return
        
        logger.info("ðŸš€ Starting HIGH VOLUME data streaming to AWS IoT Core...")
        logger.info(f"ðŸ“Š Streaming {len(self.telemetry_data):,} records over {config.PIPELINE_DURATION_HOURS} hours")
        logger.info(f"âš¡ Rate: {config.RECORDS_PER_MINUTE} records/minute ({config.RECORDS_PER_MINUTE/60:.1f} records/second)")
        
        self.streaming_stats['start_time'] = datetime.now()
        
        # Group records by device for batch processing
        device_records = {}
        for record in self.telemetry_data:
            device_id = record['device_id']
            if device_id in self.device_clients:
                if device_id not in device_records:
                    device_records[device_id] = []
                device_records[device_id].append(record)
        
        logger.info(f"ðŸ“Š Records grouped by {len(device_records)} devices")
        
        # Process records in batches
        batch_start_time = time.time()
        records_processed = 0
        
        for i in range(0, len(self.telemetry_data), config.BATCH_SIZE):
            batch = self.telemetry_data[i:i + config.BATCH_SIZE]
            batch_success_count = 0
            
            # Process batch
            for record in batch:
                device_id = record['device_id']
                
                if device_id not in self.device_clients:
                    self.streaming_stats['failed_records'] += 1
                    continue
                
                # Send single record (keeping original logic)
                client = self.device_clients[device_id]
                sent = client.send_telemetry_batch([record])
                
                if sent > 0:
                    self.streaming_stats['sent_records'] += 1
                    batch_success_count += 1
                else:
                    self.streaming_stats['failed_records'] += 1
            
            records_processed += len(batch)
            self.streaming_stats['batches_processed'] += 1
            
            # Progress reporting
            if records_processed % config.PROGRESS_REPORT_INTERVAL == 0:
                self.report_progress(records_processed)
            
            # Rate limiting - maintain target rate
            current_time = time.time()
            expected_time = batch_start_time + (records_processed * config.DELAY_BETWEEN_RECORDS)
            
            if current_time < expected_time:
                sleep_time = expected_time - current_time
                time.sleep(sleep_time)
            
            # Health check every 1000 records
            if records_processed % 1000 == 0:
                self.health_check()
        
        # Final statistics
        self.final_report()
    
    def report_progress(self, records_processed):
        """Report streaming progress"""
        elapsed = (datetime.now() - self.streaming_stats['start_time']).total_seconds()
        progress_pct = records_processed / len(self.telemetry_data) * 100
        
        # Calculate rates
        records_per_second = records_processed / elapsed if elapsed > 0 else 0
        records_per_minute = records_per_second * 60
        
        # Estimate completion
        remaining_records = len(self.telemetry_data) - records_processed
        remaining_seconds = remaining_records / records_per_second if records_per_second > 0 else 0
        completion_time = datetime.now() + timedelta(seconds=remaining_seconds)
        
        # Success rate
        success_rate = self.streaming_stats['sent_records'] / records_processed * 100 if records_processed > 0 else 0
        
        logger.info(f"ðŸ“ˆ PROGRESS: {records_processed:,}/{len(self.telemetry_data):,} ({progress_pct:.1f}%)")
        logger.info(f"âš¡ Rate: {records_per_minute:.1f}/min | {records_per_second:.1f}/sec")
        logger.info(f"âœ… Success: {success_rate:.1f}% | ETA: {completion_time.strftime('%H:%M:%S')}")
    
    def health_check(self):
        """Check health of device connections"""
        unhealthy_devices = []
        for device_id, client in self.device_clients.items():
            if not client.is_healthy():
                unhealthy_devices.append(device_id)
        
        if unhealthy_devices:
            logger.warning(f"âš ï¸ {len(unhealthy_devices)} devices unhealthy")
            # Could implement reconnection logic here
    
    def final_report(self):
        """Generate final streaming report"""
        elapsed_total = (datetime.now() - self.streaming_stats['start_time']).total_seconds()
        
        logger.info(f"\nðŸŽ‰ HIGH VOLUME STREAMING COMPLETED!")
        logger.info(f"ðŸ“Š FINAL STATISTICS:")
        logger.info(f"   â±ï¸ Total Duration: {elapsed_total/3600:.2f} hours ({elapsed_total/60:.1f} minutes)")
        logger.info(f"   ðŸ“¨ Total Records: {len(self.telemetry_data):,}")
        logger.info(f"   âœ… Successfully Sent: {self.streaming_stats['sent_records']:,}")
        logger.info(f"   âŒ Failed: {self.streaming_stats['failed_records']:,}")
        logger.info(f"   ðŸ“ˆ Success Rate: {self.streaming_stats['sent_records']/len(self.telemetry_data)*100:.2f}%")
        logger.info(f"   ðŸš€ Average Rate: {self.streaming_stats['sent_records']/(elapsed_total/60):.1f} records/minute")
        logger.info(f"   ðŸ“± Devices Used: {len(self.device_clients)}")
        logger.info(f"   ðŸ“¦ Batches Processed: {self.streaming_stats['batches_processed']:,}")
    
    def disconnect_all_devices(self):
        """Disconnect all devices from AWS IoT Core"""
        logger.info("ðŸ”Œ Disconnecting all devices...")
        for device_id, client in self.device_clients.items():
            client.disconnect()
        self.device_clients.clear()
        logger.info("âœ… All devices disconnected")

def main():
    """Main execution function for high volume streaming"""
    streamer = AWSIoTHighVolumeStreamer()
    
    try:
        print("=== AWS IoT Core HIGH VOLUME Telematics Streaming ===")
        print(f"ðŸ“ Data file: {config.DATA_FILE}")
        print(f"ðŸŽ¯ Target: {config.TOTAL_RECORDS:,} records in {config.PIPELINE_DURATION_HOURS} hours")
        print(f"âš¡ Rate: {config.RECORDS_PER_MINUTE} records/minute")
        print()
        
        # Step 1: Load device connections
        logger.info("Step 1: Loading device connections...")
        if not streamer.load_device_connections():
            return False
        
        # Step 2: Load telemetry data  
        logger.info("Step 2: Loading high volume telemetry data...")
        if not streamer.load_telemetry_data():
            return False
        
        # Step 3: Connect devices to AWS IoT Core
        logger.info("Step 3: Connecting devices to AWS IoT Core...")
        if not streamer.connect_devices():
            logger.error("âŒ Failed to connect any devices")
            return False
        
        # Step 4: Stream high volume data
        logger.info("Step 4: Starting HIGH VOLUME data streaming...")
        streamer.stream_data_high_volume()
        
        return True
        
    except KeyboardInterrupt:
        logger.info("ðŸ›‘ High volume streaming interrupted by user")
        return False
    except Exception as e:
        logger.error(f"âŒ High volume streaming failed: {str(e)}")
        return False
    finally:
        # Always disconnect devices
        streamer.disconnect_all_devices()

if __name__ == "__main__":
    # Prerequisites check for high volume
    print("ðŸ” HIGH VOLUME Prerequisites check:")
    
    # Check if high volume data file exists
    if os.path.exists(config.DATA_FILE):
        # Get file size
        file_size = os.path.getsize(config.DATA_FILE) / (1024 * 1024)  # MB
        print(f"âœ… High volume data file found: {config.DATA_FILE}")
        print(f"ðŸ“ File size: {file_size:.1f} MB")
    else:
        print(f"âŒ High volume data file not found: {config.DATA_FILE}")
        print("ðŸ‘‰ Please check the file path!")
        exit(1)
    
    # Check certificates
    if os.path.exists(config.CERT_DIR):
        cert_files = len([f for f in os.listdir(config.CERT_DIR) if f.endswith('.pem.crt')])
        print(f"âœ… Certificates directory found with {cert_files} certificate files")
        
        if cert_files == 0:
            print("âŒ No certificate files found!")
            exit(1)
    else:
        print(f"âŒ Certificates directory not found: {config.CERT_DIR}")
        exit(1)
    
    print()
    print("ðŸš€ Starting 5-hour high volume streaming pipeline...")
    
    # Run high volume streaming
    success = main()
    
    if success:
        print("\nðŸŽ‰ âœ… HIGH VOLUME AWS IoT Core streaming completed successfully!")
        print("ðŸ“Š Check AWS IoT Core console for streaming metrics")
        print("ðŸ‘‰ Monitor topic: telematics/+/data")
    else:
        print("\nâŒ HIGH VOLUME AWS IoT Core streaming failed!")
        print("ðŸ‘‰ Check aws_iot_streaming.log for detailed error analysis")