from dora import Node

import logging
import os
import random
import threading
import time

import numpy as np
import pyarrow as pa


def read_data_task(node, log):
    """Task that reads incoming events."""
    while (event := node.next()) is not None:
        if event["type"] == "INPUT" and event["id"] == "data":
            print(f"info {event['value'].to_numpy()}")
        if event["type"] == "STOP":
            break
        del event
    log.log(logging.INFO, "read_data_task done!")


def publish_task(node, log):
    """Task that publishes to a topic."""
    while True:
        time.sleep(1)  # Publish every 1s
        now = time.perf_counter_ns()
        node.send_output("data", pa.array([np.uint64(now)]))


def exit_after_delay():
    """Kill this process after 30–60s to test restart behavior."""
    time.sleep(random.randint(30, 60))
    os._exit(0)


def main():
    node = Node()
    log = logging.getLogger(__name__)

    # Kill process after 30–60s so restart can be tested (os._exit avoids waiting on threads)
    # timer = threading.Thread(target=exit_after_delay, daemon=True)
    # timer.start()

    # Create thread for read task
    read_thread = threading.Thread(target=read_data_task, args=(node, log))
    read_thread.start()

    # Run publish task in a daemon thread (so it doesn't block main thread)
    publish_thread = threading.Thread(target=publish_task, args=(node, log), daemon=True)
    publish_thread.start()

    # Block forever; process is killed by timer
    read_thread.join()
    exit(1)


if __name__ == "__main__":
    main()