from typing import AsyncGenerator, Tuple, Optional
import asyncio
import aiohttp
import dag_cbor
import io
import sqlite3
import time
import traceback
from carfile import enumerate_car

class FirehoseClient:
	def __init__(self, host: str, cur: sqlite3.Cursor, client: aiohttp.ClientSession) -> None:
		self.host = host
		self.client = client

		cur.execute("""CREATE TABLE IF NOT EXISTS firehoses (
			firehose_host TEXT PRIMARY KEY NOT NULL,
			firehose_last_seq INTEGER NOT NULL
		)""")

		cur.execute("INSERT OR IGNORE INTO firehoses (firehose_host, firehose_last_seq) VALUES (?, 0)", (self.host,))
		cur.connection.commit()
		self.last_commit = time.time()

		self.cursor = cur.execute("SELECT firehose_last_seq FROM firehoses WHERE firehose_host=?", (self.host,)).fetchone()[0]

		self.cur = cur
	
	async def __aenter__(self):
		return self

	async def __aexit__(self, *excinfo):
		await self.client.close()

	# wrapped by record_events() to provide graceful error recovery
	async def _record_events_raw(self) -> AsyncGenerator[Tuple[str, str, Optional[dict]], None]:
		async with self.client.ws_connect(f"wss://{self.host}/xrpc/com.atproto.sync.subscribeRepos?cursor={self.cursor}", timeout=15) as ws:
			while True:
				msg = io.BytesIO(await ws.receive_bytes())
				header = dag_cbor.decode(msg, allow_concat=True)
				if header.get("t") != "#commit": # we only care about commits (for now)
					continue
				body = dag_cbor.decode(msg)
				root, records = enumerate_car(io.BytesIO(body["blocks"]))
				records = dict(records)
				for op in body.get("ops", []):
					record = records.get(op["cid"])
					if record is not None:
						record = dag_cbor.decode(record)
					yield op["action"], "at://" + body["repo"] + "/" + op["path"], record
				self.cursor = body["seq"]
				if (time.time() - self.last_commit) > 10: # only write updates every 10 seconds, so we don't cause unnecessary disk churn while catching up on backlog
					self.cur.execute("UPDATE firehoses SET firehose_last_seq=? WHERE firehose_host=?", (self.cursor, self.host))
					self.cur.connection.commit()
					self.last_commit = time.time()

	# (action, at_uri, record_value)
	async def listen_for_record_events(self) -> AsyncGenerator[Tuple[str, str, Optional[dict]], None]:
		while True:
			try: # TODO: add watchdog timeout for hung connections?
				async for event in self._record_events_raw(): # apparently async `yield from` isn't a thing
					yield event
			except asyncio.exceptions.CancelledError as e: # usually a second-order effect of me pressing ctrl+C
				raise e
			except KeyboardInterrupt as e:
				raise e
			except: # TODO: maybe some error types we don't want to reconnect on
				RECONNECT_DELAY_SECS = 10  # TODO: exponential backoff?
				traceback.print_exc()
				print(f"WS error - reconnecting to {self.host} in {RECONNECT_DELAY_SECS} seconds")
				await asyncio.sleep(RECONNECT_DELAY_SECS)

if __name__ == "__main__":
	async def main():
		con = sqlite3.connect("test.db")
		cur = con.cursor()
		async with aiohttp.ClientSession() as webclient:
			client = FirehoseClient("bsky.social", cur, webclient)
			async for commit in client.listen_for_record_events():
				print("commit", commit)

	asyncio.run(main())
