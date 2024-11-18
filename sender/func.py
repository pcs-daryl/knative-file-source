import os
import csv
import json
import shutil
import pika
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from .env file
load_dotenv()

# Get configurations from environment variables
input_directory = os.getenv("CSV_DIRECTORY")
output_directory = os.getenv("CSV_OUTPUT_DIRECTORY")
amqp_url = os.getenv("AMQP_URL")
exchange_name = os.getenv("AMQP_EXCHANGE")
routing_key = os.getenv("AMQP_ROUTING_KEY")

if not all([input_directory, output_directory, amqp_url, exchange_name, routing_key]):
    raise EnvironmentError(
        "Ensure CSV_DIRECTORY, CSV_OUTPUT_DIRECTORY, AMQP_URL, AMQP_EXCHANGE, and AMQP_ROUTING_KEY are set in the environment."
)


class CsvFileHandler(FileSystemEventHandler):
    """
    Handler for processing new or modified CSV files in the directory.
    """

    def __init__(self, amqp_url, exchange, routing_key, output_directory):
        self.amqp_url = amqp_url
        self.exchange = exchange
        self.routing_key = routing_key
        self.output_directory = output_directory

    # def publish_to_amqp(self, file_path): 
    #     """
    #     Reads a CSV file and publishes its rows to the AMQP exchange, then moves it to the output folder. 13/s
    #     """
    #     try:
    #         # Establish connection to the AMQP server
    #         connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
    #         channel = connection.channel()

    #         # Ensure the exchange exists
    #         channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=True)

    #         # Read the CSV file
    #         with open(file_path, mode='r') as csvfile:
    #             reader = csv.reader(csvfile)
    #             headers = next(reader)  # Get the header row
    #             for row in reader:
    #                 # Combine headers and row data into a dictionary for structured output
    #                 row_dict = dict(zip(headers, row))
                    
    #                 # Convert each row to JSON
    #                 message = json.dumps(row_dict)

    #                 # Publish the message
    #                 channel.basic_publish(exchange=self.exchange, 
    #                                     routing_key=self.routing_key, 
    #                                     body=message)
    #                 print(f"Published message from {file_path}: {message}")

    #         # Close the connection
    #         connection.close()

    #         # Move the processed file to the output folder
    #         self.move_to_output(file_path)

    #     except Exception as e:
    #         print(f"Failed to process file {file_path}: {e}")

    def publish_to_amqp(self, file_path):
        """
        Reads a CSV file using pandas and publishes its rows to the AMQP exchange, then moves it to the output folder.
        """
        try:
            # Establish connection to the AMQP server
            connection = pika.BlockingConnection(pika.URLParameters(self.amqp_url))
            channel = connection.channel()

            # Ensure the exchange exists
            channel.exchange_declare(exchange=self.exchange, exchange_type='direct', durable=True)

            # Read the CSV file into a pandas DataFrame
            df = pd.read_csv(file_path)

            def publish_message(row):
                message = json.dumps(row.to_dict())

                # Publish the message
                channel.basic_publish(exchange=self.exchange, 
                                    routing_key=self.routing_key, 
                                    body=message)
                print(f"Published message from {file_path}: {message}")

            df.apply(lambda row:  publish_message(row), axis =1)

            # Close the connection
            connection.close()

            # Move the processed file to the output folder
            self.move_to_output(file_path)

        except Exception as e:
            print(f"Failed to process file {file_path}: {e}")

    


    def move_to_output(self, file_path):
        """
        Moves the processed CSV file to the output directory.
        """
        try:
            if not os.path.exists(self.output_directory):
                os.makedirs(self.output_directory)
            
            shutil.move(file_path, os.path.join(self.output_directory, os.path.basename(file_path)))
            print(f"Moved {file_path} to {self.output_directory}")

        except Exception as e:
            print(f"Failed to move file {file_path}: {e}")

    def on_created(self, event):
        """
        Triggered when a new file is created in the directory.
        """
        if event.src_path.endswith('.csv'):
            print(f"New CSV file detected: {event.src_path}")
            self.publish_to_amqp(event.src_path)

    def on_modified(self, event):
        """
        Triggered when a file is modified in the directory.
        """
        if event.src_path.endswith('.csv'):
            print(f"Modified CSV file detected: {event.src_path}")
            self.publish_to_amqp(event.src_path)


def monitor_directory(input_directory, output_directory, amqp_url, exchange, routing_key):
    """
    Sets up a directory watcher to monitor for new/modified CSV files.
    """
    event_handler = CsvFileHandler(amqp_url, exchange, routing_key, output_directory)
    observer = Observer()
    observer.schedule(event_handler, input_directory, recursive=False)
    observer.start()

    print(f"Monitoring directory: {input_directory}")
    try:
        while True:
            pass  # Keep the program running
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


# Run the program
monitor_directory(input_directory, output_directory, amqp_url, exchange_name, routing_key)
