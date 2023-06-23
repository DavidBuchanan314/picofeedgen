from typing import Optional
from feedgen import FeedGenerator
import sqlite3

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

# a list of known bot DIDs, to be excluded from the feed
# if this list was longer, it might make sense to have it be a set()
ROBOTS = [
	"did:plc:p64u6mcz2x2qnvrktq4gekja", # load.sandbox.whyr.us (occasional load-test posting bursts)
	"did:plc:6vouvht32sd5aojpjyxjus2e", # beep.sandbox.whyr.us (one post per second)
]

class HumansOnlyFeed(FeedGenerator):
	def __init__(self) -> None:
		self.con = sqlite3.connect("human_posts.db")
		self.cur = self.con.cursor()

		# we don't bother indexing the content of posts, only when and by whom
		self.cur.execute("""CREATE TABLE IF NOT EXISTS posts (
			post_aturi TEXT PRIMARY KEY NOT NULL,
			post_author_did TEXT NOT NULL,
			post_timestamp_ns INTEGER NOT NULL
		)""")

		# we might need to delete all posts from a certain author
		# TODO: add index

		# we'll be using timestamps as a cursor
		# TODO: add index

		# housekeeping:
		# ROBOTS may have been updated, delete any previous posts from them
		self.cur.executemany("DELETE FROM posts WHERE post_author_did=?", [(r,) for r in ROBOTS])

		self.con.commit()
	
	def get_feed(self, requester_did: str, limit: int, cursor: Optional[str]=None) -> dict:
		# placeholder static feed that only returns a single post!
		return {
			"feed": [
				{"post": "at://did:plc:fzgsygoeg2ydv73mlu76o54x/app.bsky.feed.post/3jytrwyoh5qdy"}
			]
		}

	def process_event(self, event) -> None:
		raise Exception("TODO")
