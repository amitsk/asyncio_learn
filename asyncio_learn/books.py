# https://openlibrary.org/api/books?bibkeys=ISBN:0385472579&format=json
import asyncio
from loguru import logger
import re
import sys
from typing import IO
import urllib.error
import urllib.parse

import aiohttp
from aiohttp import ClientSession
from typing import Dict


async def fetch_book_details(
    isbn: str, session: ClientSession, **kwargs
) -> Dict[str, str]:
    """GET request wrapper to fetch page HTML.

    kwargs are passed to `session.request()`.
    """

    url = f"https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json"
    # Don't do any try/except here.  If either the request or reading
    # of bytes raises, let that be handled by caller.
    resp = await session.request(method="GET", url=url, **kwargs)
    resp.raise_for_status()  # raise if status >= 400
    logger.info("Got response [{}] for URL: {}", resp.status, url)
    json = await resp.json()  # For bytes: resp.read()

    # Dont close session; let caller decide when to do that.
    return json


async def bulk_fetch_book_details(isbns: set, **kwargs) -> None:
    """Crawl & write concurrently to `file` for multiple `urls`."""
    async with ClientSession() as session:
        tasks = []
        for isbn in isbns:
            tasks.append(fetch_book_details(isbn=isbn, session=session, **kwargs))
        return await asyncio.gather(*tasks)  # see also: return_exceptions=True


if __name__ == "__main__":
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+."
    here = pathlib.Path(__file__).parent

    with open(here.joinpath("isbns.txt")) as infile:
        isbns = set(map(str.strip, infile))

    all_books = asyncio.run(bulk_fetch_book_details(isbns=isbns))
    logger.info(all_books)
