from typing import Optional, Tuple
from feedgen import FeedGenerator
import sqlite3
import time

"""
s.repo_put_record("app.bsky.feed.generator", {
	"$type":"app.bsky.feed.generator",
	"did":"did:web:feeds.dev.retr0.id",
	#"avatar":{"$type":"blob","ref":{"$link":"bafkrei..."},"mimeType":"image/png","size":31337},
	"createdAt": get_timestamp(),
	"description": 'Like the "All of BGS" feed, but without bots. The bot list is manually curated.',
	"displayName": "Humans of BGS"
}, rkey="humans")
"""

class HumansOnlyFeed(FeedGenerator):
	def __init__(self, robots) -> None:
		self.robots = robots
		self.con = sqlite3.connect("human_posts.db")
		self.con.execute("pragma journal_mode=wal")
		self.cur = self.con.cursor()

		# we don't bother indexing the content of posts, only when and by whom
		self.cur.execute("""CREATE TABLE IF NOT EXISTS posts (
			post_aturi TEXT PRIMARY KEY NOT NULL,
			post_author_did TEXT NOT NULL,
			post_timestamp_ns INTEGER NOT NULL
		)""")

		# we might need to delete all posts from a certain author
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_author ON posts (post_author_did)")

		# we'll be using timestamps as a cursor
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_time ON posts (post_timestamp_ns)")

		# housekeeping:
		# ROBOTS may have been updated, delete any previous posts from them
		self.cur.executemany("DELETE FROM posts WHERE post_author_did=?", [(r,) for r in self.robots])

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
		event_type, event_aturi, _ = event
		event_did, event_collection, _ = event_aturi.removeprefix("at://").split("/")
		if event_collection != "app.bsky.feed.post": # we only care about posts
			return
		if event_did in self.robots: # we don't care about robots
			return
		
		if event_type == "create":
			self.cur.execute("""INSERT OR IGNORE INTO posts (
				post_aturi,
				post_author_did,
				post_timestamp_ns
			) VALUES (?, ?, ?)""", (event_aturi, event_did, int(time.time()*1000000)))
		elif event_type == "delete":
			self.cur.execute("DELETE FROM posts WHERE post_aturi=?", (event_aturi,))
		
		# housekeeping: delete old posts
		self.cur.execute("DELETE FROM posts WHERE post_timestamp_ns<?", ((time.time()-24*60*60)*1_000_000,))
		self.con.commit()
