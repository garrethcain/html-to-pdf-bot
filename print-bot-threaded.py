import os
import sys
import json
import time
import pika
import signal
import logging
import functools
import threading
from celery import Celery
from google.cloud import storage
from playwright.sync_api import sync_playwright
from concurrent.futures import ThreadPoolExecutor


PREFETCH = int(os.getenv("PREFETCH", "4"))
print_queue = os.getenv("PRINT_QUEUE", "pdf_printer")
RABBITMQ_HOSTNAME = os.getenv("RABBITMQ_HOSTNAME", "127.0.0.1")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "rabbit")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "rabbit")

kwargs = {"print_background": True, "margin":{"top":"0.0","left":"0.0","right":"0", "bottom":"0"}, "scale": 0.9} 
LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

futures = []

class Tls(threading.local):
    def __init__(self) -> None:
        self.playwright = sync_playwright().start()
        print("Creating playwright instance in Thread:", threading.current_thread().name)

    def __del__(self):
        print("destroying worker...")
        self.tls.playwright.stop()


class Worker:
    tls = Tls()

    def __init__(self, connection, channel, delivery_tag, body) -> None:
        self.delivery_tag = delivery_tag
        self.connection = connection
        self.channel = channel
        self.body = body

    def run(self) -> None:
        LOGGER.info(f'Delivery tag: {self.delivery_tag} Message body: {self.body} in thread {threading.current_thread().name}')

        error = ""

        try:
            body = json.loads(self.body)
            content = self.get_html(body)
            sizeInput = sys.getsizeof(content)

            browser = self.tls.playwright.chromium.launch(headless=True)
            context = browser.new_context()
            page = browser.new_page()
            page.set_default_navigation_timeout(300000) # 5minutes.

            page.set_content(content)
            pdf_bytes = page.pdf(**kwargs)
            
            page.close()
            context.close()
            browser.close()

            self.upload_pdf(body, pdf_bytes)
            status = "COMPLETE"
        except Exception as e:
            error = f"{e.message}" if hasattr(e, 'message') else f"{e}"
            status = "FAILED"

        result = {
            "jobId": body["jobId"],
            "jobName": body["jobName"],
            "status": status,
            "error": error,
        }

        LOGGER.info(f'Delivery tag: {self.delivery_tag} Result body: {result}')
        cb = functools.partial(self.ack_message, self.channel, self.delivery_tag)
        connection.add_callback_threadsafe(cb)

    def ack_message(self, channel, delivery_tag):
        """
        Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            exit() # let k8s worry about it, the msg will be returned per AMQP spec.

    def get_blob(self, file_path, bucket_name, body):
        client = storage.Client()
        bucket = client.get_bucket(body[bucket_name])
        return storage.Blob(name=body[file_path], bucket=bucket)

    def upload_pdf(self, body, pdf_bytes):
        blob = self.get_blob("destinationPath", "destinationBucket", body )
        mimetype = "application/pdf"
        blob.upload_from_string(
            pdf_bytes, mimetype
        )

    def get_html(self, body):
        blob = self.get_blob("sourcePath", "sourceBucket", body)
        if not blob.exists():
            raise("Does not exist")
        return blob.download_as_string().decode("utf-8")

    def job_complete(self, body, payload, channel):
        """
        Sending job complete back to celery's default queue.
        """
        app = Celery(
            "PythonDB", 
            task_soft_time_limit=300, 
            broker=f"pyamqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOSTNAME}", 
        )
        payload = {
            "body": json.dumps(payload)
        }
        app.send_task(name="categorisation.tasks.print_job_response", kwargs=payload, queue="celery")


def on_message(channel, method_frame, header_frame, body, args):
    (connection, executor) = args
    delivery_tag = method_frame.delivery_tag
    worker = Worker(connection, channel, delivery_tag, body)
    futures.append(
        executor.submit(worker.run)
    )

## Handle sigterm from k8s.
def receiveSignal(signalNumber, frame):
    print("SIGTERM received. Shutting down gracefully.")
    channel.stop_consuming()
    return

credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
parameters =  pika.ConnectionParameters(RABBITMQ_HOSTNAME, credentials=credentials, heartbeat=5, socket_timeout=5, blocked_connection_timeout=5)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=print_queue, durable=True)
channel.basic_qos(prefetch_count=PREFETCH)


executor = ThreadPoolExecutor(max_workers=5)
on_message_callback = functools.partial(on_message, args=(connection, executor))
channel.basic_consume(queue=print_queue, on_message_callback=on_message_callback)
signal.signal(signal.SIGTERM, receiveSignal)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    channel.stop_consuming()

executor.shutdown()
connection.close()
print("Good-bye world.")
