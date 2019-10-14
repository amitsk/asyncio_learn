import time
import trio


async def broken_double_sleep(x):
    print("*yawn* Going to sleep")
    start_time = time.perf_counter()

    # Whoops, we forgot the 'await'!
    trio.sleep(2 * x)

    sleep_time = time.perf_counter() - start_time
    print("Woke up after {:.2f} seconds, feeling well rested!".format(
        sleep_time))


trio.run(broken_double_sleep, 3)