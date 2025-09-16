import os
import asyncio
from zlib import decompress
## TODO note i had to refactor/rename logging.py to dap_logging.py
## because asyncio depends on another package called logging.py?
from dap.api import DAPClient
from dap.dap_types import Credentials, SnapshotQuery, Format
from dap.log import configure_logging
from dotenv import load_dotenv


async def download_table_from_dap(table_name, download_folder):
    load_dotenv(".env")
    client_id: str = os.environ["DAP_CLIENT_ID"]
    client_secret: str = os.environ["DAP_CLIENT_SECRET"]
    ## TODO do I need above 2 vars if .env is loaded?
    os.makedirs(download_folder, exist_ok=True)
    configure_logging()
    async with DAPClient() as session:  # should auto grab stuff from .env?
        query = SnapshotQuery(format=Format.JSONL, mode=None)
        await session.download_table_data(      # didnt want to work outside async func
            "canvas", table_name, query, download_folder, decompress=True
        )

## TODO make a giant pandas df out of the 5 part json job (submissions)
## and compare to what is in the database (database most recent graded_at is 2024?)
## use sqlaclhemy for database interaction? pandas is what I know but might be better options

# if __name__ == "__main__":
#     asyncio.run(main())


