from typing import AsyncGenerator, Tuple, Optional
import asyncio
import aiohttp
import dag_cbor
import io
import sqlite3
import time

class FirehoseClient:
	def __init__(self, host: str, cur: sqlite3.Cursor) -> None:
		self.host = host
		self.client = aiohttp.ClientSession()

		cur.execute("""CREATE TABLE IF NOT EXISTS firehoses (
			firehose_host TEXT PRIMARY KEY NOT NULL,
			firehose_last_seq INTEGER NOT NULL
		)""")

		cur.execute("INSERT OR IGNORE INTO firehoses (firehose_host, firehose_last_seq) VALUES (?, 0)", (self.host,))
		cur.connection.commit()
		self.last_commit = time.time()

		self.cursor = cur.execute("SELECT firehose_last_seq FROM firehoses WHERE firehose_host=?", (self.host,)).fetchone()[0]

		self.cur = cur
	
	# (action, at_uri, record_value)
	async def record_events(self) -> AsyncGenerator[Tuple[str, str, Optional[dict]], None]:
		while True:
			try: # TODO: add watchdog timeout for hung connections?
				async with self.client.ws_connect(f"wss://{self.host}/xrpc/com.atproto.sync.subscribeRepos?cursor={self.cursor}") as ws:
					while True:
						msg = io.BytesIO(await ws.receive_bytes())
						header = dag_cbor.decode(msg, allow_concat=True)
						if header.get("t") != "#commit": # we only care about commits
							continue
						body = dag_cbor.decode(msg)
						for op in body.get("ops", []):
							record = {} # TODO!!!! extract record value from CAR
							yield op["action"], "at://" + body["repo"] + "/" + op["path"], record
						self.cursor = body["seq"]
						if (time.time() - self.last_commit) > 10: # only write updates every 10 seconds, so we don't cause unnecessary disk churn while catching up on backlog
							self.cur.execute("UPDATE firehoses SET firehose_last_seq=? WHERE firehose_host=?", (self.cursor, self.host))
							self.cur.connection.commit()
							self.last_commit = time.time()
			except aiohttp.WebSocketError: # TODO: include more exception types?
				print(f"WS error - reconnecting to {self.host} in 10 seconds")
				asyncio.sleep(10)

if __name__ == "__main__":
	async def main():
		con = sqlite3.connect("test.db")
		cur = con.cursor()
		client = FirehoseClient("bsky.social", cur)
		async for commit in client.record_events():
			print("commit", commit)

	asyncio.run(main())
