import asyncio
import attr
import string
import random
from loguru import logger
import uuid


@attr.s
class PubSubMessage:
    instance_name = attr.ib()
    message_id = attr.ib(repr=False)
    hostname = attr.ib(repr=False, init=False)

    def __attrs_post_init__(self):
        self.hostname = f"{self.instance_name}.example.net"


async def publish(queue, publisher_id):
    choices = string.ascii_lowercase + string.digits
    while True:
        msg_id = str(uuid.uuid4())
        host_id = "".join(random.choices(choices, k=4))
        instance_name = f"cattle-{host_id}"
        msg = PubSubMessage(message_id=msg_id, instance_name=instance_name)
        # publish an item
        asyncio.create_task(queue.put(msg))
        logger.info(f"[{publisher_id}] Published message {msg}")
        # simulate randomness of publishing messages
        await asyncio.sleep(random.random())


async def consume(queue):
    while True:
        # wait for an item from the publisher
        msg = await queue.get()

        # the publisher emits None to indicate that it is done
        if msg is None:
            break

        # process the msg
        logger.info(f"Consumed {msg}")
        # simulate i/o operation using sleep
        await asyncio.sleep(random.random())


def main():
    queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    coros = [publish(queue, i) for i in range(1, 4)]

    try:
        [loop.create_task(coro) for coro in coros]
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Process interrupted")
    finally:
        loop.close()
        logger.info("Successfully shutdown the Mayhem service.")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()