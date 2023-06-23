from abc import ABC, abstractmethod
from typing import Optional

"""
If you're writing non-trivial feed algorithms, you might want to refactor this
to make the methods async.

If you have multiple feeds, you might also want to build a centralised DB
for any state that's relevant to all feeds (e.g. like counts of posts)
"""

class FeedGenerator(ABC):
	@abstractmethod
	def get_feed(self, requester_did: str, limit: int, cursor: Optional[str]=None) -> dict:
		"""
		returns a getFeedSkeleton response object
		"""
		pass

	@abstractmethod
	def process_event(self, event) -> None:
		pass
