import os
import sys
import json
import pika
import signal
import shlex
import logging
import functools
import threading
import subprocess

from pathlib import Path
from celery import Celery
from pika.spec import Channel
from google.cloud import storage
from pika.connection import Connection
from playwright.sync_api import sync_playwright


PREFETCH = int(os.getenv("PREFETCH", "4"))
print_queue = os.getenv("PRINT_QUEUE", "printer")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "rabbit")
RABBITMQ_HOSTNAME = os.getenv("RABBITMQ_HOSTNAME", "127.0.0.1")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "rabbit")

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) -35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)


def msg_celery(status: str, error: str, body: dict):
    result = {
        "jobId": body["jobId"],
        "jobName": body["jobName"],
        "status": status,
        "error": error,
        "profiling": {
            "launch": -1,
            "duration": -1,
            "retrieval": -1,
            "sizeInput": -1,
            "render": -1,
            "upload": -1,
            "sizeOutput": -1,
            "total": -1
        }
    }
    app = Celery(
        "PythonDB",
        task_soft_time_limit=300,
        broker=f"pyamqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOSTNAME}",
    )
    payload = {
        "body": json.dumps(result)
    }
    app.send_task(name="categorisation.tasks.print_job_response", kwargs=payload, queue="celery")
    LOGGER.info(f'Message: {status} Job: {body["jobId"]} Result: {result}')

def ack_message(channel, delivery_tag):
    """
    Note that `channel` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        sys.exit() # let k8s worry about it.

def nack_message(channel: Channel, delivery_tag: str, body: dict):
    """
    According the the AMPQ spec, a nack is just an ack and a
    requeue. So, to create a kind of BGP max_hop TTL type thingy
    we''ve written our own nack process.
    """
    if channel.is_open:
        # ack the original message.
        ack_message(channel, delivery_tag)
        # safety switch to stop endless requeuing.
        body["retry"] = int(body.get("retry", "1")) - 1
        if body["retry"] > 0:
            print("nacking")
            channel.basic_publish(
                exchange="", routing_key=print_queue, body=json.dumps(body)
            )
        else:
            # Send final update.
            msg_celery("DIED", "Retry limit reached", body)
        LOGGER.info(f'Status: nack, Job: {body["jobId"]}, {body["retry"]} tries left.')
    else:
        sys.exit() # let k8s worry about it.

def get_bucket(bucket_name):
    client = storage.Client()
    return client.get_bucket(bucket_name)

def upload_blob(body, bytes, mimetype):
    bucket = get_bucket(body["destinationBucket"])
    blob = storage.Blob(name=body["destinationPath"], bucket=bucket)
    blob.upload_from_string(
        bytes, mimetype
    )

def get_blob(body: dict):
    bucket = get_bucket(body["sourceBucket"])
    blob = storage.Blob(name=body["sourcePath"], bucket=bucket)
    if not blob.exists():
        raise Exception("Does not exist")
    return blob.download_as_string()

def render_pdf(connection: Connection, channel: Channel, delivery_tag: str, body: dict) -> None:
    error:str = None
    status:str = None

    try:
        # this was at the top, but here now for easy access.
        args = {"handle_sigterm": False, "handle_sighup": False, "timeout": 30000}
        kwargs = {"print_background": True, "margin":{"top":"0.0","left":"0.0","right":"0", "bottom":"0"}, "scale": 0.9}
        content: str = get_blob(body).decode("utf-8")
        with sync_playwright() as p:
            browser = p.chromium.launch(**args)
            page = browser.new_page()
            page.set_default_timeout(90000)
            page.set_content(content)
            pdf_bytes = page.pdf(**kwargs)
            page.close()
            upload_blob(body, pdf_bytes, "application/pdf")
            status = "COMPLETE"
            # ack the message as it was successfull.
            cb = functools.partial(ack_message, channel, delivery_tag)
            connection.add_callback_threadsafe(cb)
            msg_celery(status, error, body)
    except Exception as e:
        # nack and reduce the ttl
        cb = functools.partial(nack_message, channel, delivery_tag, body)
        connection.add_callback_threadsafe(cb)
        # catching one of the messages from when the instance terminates
        # while working. I'll find a propper solution to this eventually.
        error = f"{e.message}" if hasattr(e, 'message') else f"{e}"
        if error == "Target Closed":
            error = f"Requeuing job, received error: {error}"
            status = "REQUEUED"
        else:
            status = "FAILED"
    LOGGER.info(f"Status: {status} Job: {body['jobId']} Body: {error} {body}")

def render_html(connection: Connection, channel: Channel, delivery_tag: str, body: dict):
    error: str = None
    status: str = None
    try:
        json_blob = get_blob(body)
        stdin = json_blob
        executable = ["node", Path(__file__).parents[0] / "renderer/dist/index.js"]
        result = subprocess.run(
            executable,
            input=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=300,
        )
        # Try raise an exception if there was one.
        result.check_returncode()
        html_bytes = result.stdout
        upload_blob(body, html_bytes, "text/html")
        status = "COMPLETE"
        # if we got here, ack as its a success.
        cb = functools.partial(ack_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)
        msg_celery(status, error, body)
    except subprocess.CalledProcessError as e:
        status = "CALLEDPROCESSERROR"
        error = f"{e.message}" if hasattr(e, 'message') else f"{e}"
        # nack and reduce the ttl
        cb = functools.partial(nack_message, channel, delivery_tag, body)
        connection.add_callback_threadsafe(cb)
    except Exception as e:
        status = "FAILED"
        error = f"{e.message}" if hasattr(e, 'message') else f"{e}"
        # nack and reduce the ttl
        cb = functools.partial(nack_message, channel, delivery_tag, body)
        connection.add_callback_threadsafe(cb)
    # send notice back and log.
    LOGGER.info(f"Status: {status} Job: {body['jobId']} Body: {error} {body}")

def do_work(connection, channel, delivery_tag, body):
    """Choose which print job to run"""
    LOGGER.info(f"Starting: {body['jobName']} {body['jobId']} Body: {body}")

    jobs = {
        'HTML_TO_PDF': render_pdf,
        'JSON_TO_HTML': render_html,
    }
    jobs[body['jobName']](connection, channel, delivery_tag, body)

def on_message(channel, method_frame, header_frame, body, args):
    body = json.loads(body)
    (connection, threads) = args
    delivery_tag = method_frame.delivery_tag
    t = threading.Thread(target=do_work, args=(connection, channel, delivery_tag, body))
    t.start()
    threads.append(t)

## Handle sigterm from k8s.
def receiveSignal(signalNumber: int, frame: any):
    print("SIGTERM received. Shutting down gracefully.")
    channel.stop_consuming()

def on_closed(channel, reason):
    # if we just exit, the message is requeued
    # and the pod restarts.
    # sys.exit()
    print("**** connection closed")

#######
## App starts here.
#######
while True:
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(RABBITMQ_HOSTNAME, credentials=credentials, heartbeat=5, blocked_connection_timeout=600)
        connection = pika.BlockingConnection(parameters)

        threads = []
        channel = connection.channel()
        channel.queue_declare(queue=print_queue, durable=True)
        channel.basic_qos(prefetch_count=PREFETCH)

        on_message_callback = functools.partial(on_message, args=(connection, threads))
        channel.basic_consume(queue=print_queue, on_message_callback=on_message_callback)
        signal.signal(signal.SIGTERM, receiveSignal)

        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()
        # Wait for all to complete
        for thread in threads:
            thread.join()
        connection.close()
        break
    except pika.exceptions.ConnectionClosedByBroker as e:
        print(f"*** ConnectionClosedByBroker: {e}")
        break
    except pika.exceptions.AMQPChannelError as e:
        print(f"*** AMQPChannelError: {e}")
        break
    except pika.exceptions.AMQPConnectionError as e:
        print(f"*** AMQPConnectionError: {e}")
        break
print("Good-bye world.")
os._exit(os.EX_OK)
print("I'm still alive.")