from typing import Optional, Tuple
from feedgen import FeedGenerator
import sqlite3
import time

class AltTextFeed(FeedGenerator):
	def __init__(self) -> None:
		self.con = sqlite3.connect("alt_text_posts.db")
		self.cur = self.con.cursor()

		# we don't bother indexing the content of posts, only when
		self.cur.execute("""CREATE TABLE IF NOT EXISTS posts (
			post_aturi TEXT PRIMARY KEY NOT NULL,
			post_timestamp_ns INTEGER NOT NULL
		)""")

		# we'll be using timestamps as a cursor
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_time ON posts (post_timestamp_ns)")

		self.con.commit()
	
	def get_feed(self, requester_did: str, limit: int, cursor: Optional[str]=None) -> dict:
		if cursor is None:
			cursor = 999999999999999999
		posts = list(self.cur.execute("""
			SELECT post_aturi, post_timestamp_ns
			FROM posts
			WHERE post_timestamp_ns < ?
			ORDER BY post_timestamp_ns DESC
			LIMIT ?""", (int(cursor), limit)).fetchall())
		res = {
			"feed": [
				{"post": aturi}
				for aturi, _ in posts
			]
		}
		if posts:
			res["cursor"] = str(posts[-1][1])
		return res

	def process_event(self, event: Tuple[str, str, Optional[dict]]) -> None:
		event_type, event_aturi, event_record = event
		event_did, event_collection, _ = event_aturi.removeprefix("at://").split("/")
	
		if event_collection != "app.bsky.feed.post": # we only care about posts
			return
		
		if event_type != "create":
			return
		
		if event_record is None: # this shouldn't happen?
			return
		
		if "reply" in event_record: # ignore replies
			return

		if "embed" not in event_record:
			return
		
		if not event_record["embed"].get("images"):
			return
		
		imgs = event_record["embed"]["images"]

		if not all(img.get("alt") for img in imgs):
			return
		
		self.cur.execute("""INSERT OR IGNORE INTO posts (
			post_aturi,
			post_timestamp_ns
		) VALUES (?, ?)""", (event_aturi, int(time.time()*1000000)))
		self.con.commit()
