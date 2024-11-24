import os
import shutil
import pandas as pd
import pika
import json
import time
import logging
import sys
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Load environment variables from a .env file
load_dotenv()

logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)  # Write to stdout
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class CsvPublisher:
    def __init__(self, amqp_url, exchange, routing_key, output_directory, chunk_size=1000):
        self.amqp_url = amqp_url
        self.exchange = exchange
        self.routing_key = routing_key
        self.output_directory = output_directory
        self.chunk_size = chunk_size
        self.channel = None

    def connect_to_amqp(self):
        """Establish connection to AMQP server."""
        connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type='headers', durable=True)

    def publish_batch(self, batch):
        """Publish a batch of messages to AMQP."""
        messages = batch.to_dict(orient="records")  # Convert batch to list of dictionaries
        for message in messages:
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=self.routing_key,
                body=json.dumps(message)
            )
        logger.info(f"Published {len(messages)} messages.")

    def move_to_output(self, file_path):
        """Move the processed CSV file to the output directory."""
        try:
            if not os.path.exists(self.output_directory):
                os.makedirs(self.output_directory)
            
            destination_path = os.path.join(self.output_directory, os.path.basename(file_path))
            logger.info(f"Moving file from {file_path} to {destination_path}")
            shutil.move(file_path, destination_path)
            logger.info(f"Successfully moved {file_path} to {destination_path}")
        except Exception as e:
            logger.info(f"Failed to move file {file_path}: {e}")

    def process_file(self, file_path):
        """Process the CSV file and publish messages."""
        try:
            self.connect_to_amqp()

            # Read CSV in chunks and process each batch
            df = pd.read_csv(file_path)
            for start in range(0, len(df), self.chunk_size):
                batch = df.iloc[start:start + self.chunk_size]
                self.publish_batch(batch)

            # Close the connection
            self.channel.connection.close()

            # Move the processed file to the output folder
            self.move_to_output(file_path)

        except Exception as e:
            logger.info(f"Failed to process file {file_path}: {e}")

# Directory Watcher
class DirectoryWatcher(FileSystemEventHandler):
    def __init__(self, publisher):
        self.publisher = publisher

    def on_created(self, event):
        """This method is called when a new file is created in the directory."""
        if event.is_directory:
            return
        if event.src_path.endswith(".csv"):
            logger.info(f"New file detected: {event.src_path}")
            self.publisher.process_file(event.src_path)

def watch_directory(directory, publisher):
    """Watch the directory for new files and process them."""
    # Ensure the directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Directory {directory} created.")

    # Initialize the event handler and observer
    event_handler = DirectoryWatcher(publisher)
    observer = Observer()
    observer.schedule(event_handler, directory, recursive=False)
    observer.start()
    logger.info(f"Watching directory {directory} for new CSV files...")
    try:
        while True:
            time.sleep(1)  # Keep watching the directory
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

# Example usage
if __name__ == "__main__":
    # Load environment variables from .env
    amqp_url = os.getenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    exchange = os.getenv("AMQP_EXCHANGE", "my_exchange")
    routing_key = os.getenv("AMQP_ROUTING_KEY", "my_routing_key")
    input_directory = os.getenv("CSV_DIRECTORY", "./data")
    output_directory = os.getenv("CSV_OUTPUT_DIRECTORY", "./output")

    publisher = CsvPublisher(
        amqp_url=amqp_url,
        exchange=exchange,
        routing_key=routing_key,
        output_directory=output_directory
    )

    # Start watching the directory
    watch_directory(input_directory, publisher)
