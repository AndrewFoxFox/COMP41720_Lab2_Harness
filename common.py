import pika


def connect():
    return pika.BlockingConnection(pika.ConnectionParameters("localhost"))


def setup(ch):
    ch.queue_declare("embedding_tasks", durable=True)
    ch.queue_declare("embedding_results", durable=True)
