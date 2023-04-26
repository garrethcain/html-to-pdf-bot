# HTML-to-PDF-Bot

A super simple threaded service that listens for messages asking it to convert
HTML documents to PDF. It then downloads the HTML document from a GCP Bucket
converts it using Playwright then re-uploads it to a designated GCP Bucket, and
puts another message on the queue so some remote service can acknowledge it.

Depends on Pika, Celery, Playwright, Google::Cloud::Storage, threading and a few
standard libs.

It isn't meant to be an example of anything, just code snippet solving an issue
I had late one night that needed to be solved asap.