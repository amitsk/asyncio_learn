#!/usr/bin/env python3.7
# Copyright (c) 2018-2019 Lynn Root
"""
Initial setup - starting point based off of
http://asyncio.readthedocs.io/en/latest/producer_consumer.html

Notice! This requires:
 - attrs==19.1.0

To run:

    $ python part-0/mayhem_1.py

Follow along: https://roguelynn.com/words/asyncio-initial-setup/
"""

import asyncio
from loguru import logger
import random
import string

import attr


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id = attr.ib(repr=False)
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


# simulating an external publisher of events
async def publish(queue, n):
    choices = string.ascii_lowercase + string.digits

    for x in range(1, n + 1):
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=x, instance_name=f"cattle-{host_id}")
        await queue.put(msg)
        logger.info("Published {} of {} messages", x, n)

    await queue.put(None)  # publisher is done


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()
        if msg is None:  # publisher is done
            break

        # process the msg
        logger.info(f"Consumed {msg}")
        # unhelpful simulation of i/o work
        await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    try:
        loop.create_task(publish(queue, 5))
        loop.create_task(consume(queue))
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Process interrupted")
    finally:
        loop.close()
        logger.info("Successfully shutdown the Mayhem service.")


if __name__ == "__main__":
    main()