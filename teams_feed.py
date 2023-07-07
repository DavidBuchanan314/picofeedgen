from typing import Optional, Tuple
from feedgen import FeedGenerator
import sqlite3
import time
import hashlib

class TeamsFeed(FeedGenerator):
	def __init__(self, banned_dids, hash_key, team_names) -> None:
		self.banned_dids = banned_dids
		self.hash_key = hash_key
		self.team_names = team_names
		self.con = sqlite3.connect("teams_posts.db")
		self.con.execute("pragma journal_mode=wal")
		self.cur = self.con.cursor()

		# we don't bother indexing the content of posts, only when and by whom
		self.cur.execute("""CREATE TABLE IF NOT EXISTS posts (
			post_aturi TEXT PRIMARY KEY NOT NULL,
			post_author_did TEXT NOT NULL,
			post_author_team INTEGER NOT NULL,
			post_timestamp_ns INTEGER NOT NULL
		)""")

		# we might need to delete all posts from a certain author
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_author ON posts (post_author_did)")

		# we'll be using timestamps as a cursor
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_time ON posts (post_timestamp_ns)")

		# we'll be accessing team-by-team
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_team ON posts (post_author_team)")

		# not sure if this helps but more indexing probably can't hurt (famous last words)
		self.cur.execute("CREATE INDEX IF NOT EXISTS post_team_time ON posts (post_author_team, post_timestamp_ns)")

		# housekeeping:
		# banned_dids may have been updated, delete any previous posts from them
		self.cur.executemany("DELETE FROM posts WHERE post_author_did=?", [(r,) for r in self.banned_dids])

		self.con.commit()
	
	def team_for_did(self, did: str) -> int:
		# could be more cryptographically bulletproof, but meh
		# we return the first byte value, giving 256 teams
		return hashlib.sha256(did.encode() + self.hash_key).digest()[0]

	def get_feed(self, requester_did: str, limit: int, cursor: Optional[str]=None) -> dict:
		cursor_arg = cursor
		if cursor is None:
			cursor = 999999999999999999
		team = self.team_for_did(requester_did)
		posts = list(self.cur.execute("""
			SELECT post_aturi, post_timestamp_ns
			FROM posts
			WHERE post_timestamp_ns < ?
			AND post_author_team=?
			ORDER BY post_timestamp_ns DESC
			LIMIT ?""", (int(cursor), team, limit)).fetchall())
		res = {
			"feed": [
				{"post": aturi}
				for aturi, _ in posts
			]
		}
		if cursor_arg is None:
			res["feed"] = [{"post": self.team_names[team]}] + res["feed"]
		if posts:
			res["cursor"] = str(posts[-1][1])
		return res

	def process_event(self, event: Tuple[str, str, Optional[dict]]) -> None:
		event_type, event_aturi, event_data = event
		event_did, event_collection, _ = event_aturi.removeprefix("at://").split("/")
		if event_collection != "app.bsky.feed.post": # we only care about posts
			return
		if event_type != "create": # only creations
			return
		if "reply" in event_data: # no replies
			return
		if event_did in self.banned_dids: # no banned users
			return
		
		team = self.team_for_did(event_did)

		self.cur.execute("""INSERT OR IGNORE INTO posts (
			post_aturi,
			post_author_did,
			post_author_team,
			post_timestamp_ns
		) VALUES (?, ?, ?, ?)""", (event_aturi, event_did, team, int(time.time()*1000000)))

		# housekeeping: delete old posts
		self.cur.execute("DELETE FROM posts WHERE post_timestamp_ns<?", ((time.time()-24*60*60)*1_000_000,))
		self.con.commit()
