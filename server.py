from typing import Dict
import asyncio
import aiohttp_cors
from aiohttp import web
import jwt
from firehose import FirehoseClient
import logging
import sqlite3

logging.basicConfig(level=logging.DEBUG)

from config import BGS_HOST, FEED_HOSTNAME, FEED_DID, FEED_PUBLISHER_DID, FEEDS, LISTEN_HOST, LISTEN_PORT


async def hello(request: web.Request):
	return web.Response(text="Hello! This is an ATProto feed generator, running on https://github.com/DavidBuchanan314/picofeedgen")


async def did_doc(request: web.Request):
	return web.json_response({
		"@context": ["https://www.w3.org/ns/did/v1"],
		"id": FEED_DID,
		"service":[{
			"id": "#bsky_fg",
			"type": "BskyFeedGenerator",
			"serviceEndpoint": f"https://{FEED_HOSTNAME}"
		}]
	})


async def get_feed_skeleton(request: web.Request):
	# XXX: we do not verify token signatures, because we're not using the
	# requester's identity for anything important. If you want to do something important with it
	# (like making "private" or otherwise personalised feeds), you should change this.
	# Proper verification would involve requesting the user's pubkey from a PLC directory
	token = jwt.decode(request.headers["Authorization"].removeprefix("Bearer "), options={"verify_signature": False})
	requester_did = token["iss"]

	if "feed" not in request.query:
		return web.HTTPBadRequest(text="no feed specified")

	feed = request.query["feed"]
	if not feed.startswith("at://"):
		return web.HTTPBadRequest(text="feed must be a valid AT URI")
	
	aturi_parts = feed.removeprefix("at://").split("/")
	if len(aturi_parts) != 3:
		return web.HTTPBadRequest(text="feed must be a valid AT URI")
	
	feed_did, feed_collection, feed_name = aturi_parts
	if feed_collection != "app.bsky.feed.generator":
		return web.HTTPBadRequest(text="feed must reference a feed generator record")

	# XXX: I think it would technically be valid for feed_did to be a handle here
	if feed_did != FEED_PUBLISHER_DID:
		return web.HTTPNotFound(text="we don't host any feeds from that publisher")
	
	limit = int(request.query.get("limit", 50))
	cursor = request.query.get("cursor")

	if limit < 1:
		limit = 1
	elif limit > 100:
		limit = 100
	
	if feed_name not in FEEDS:
		return web.HTTPNotFound(text="feed does not exist")
	
	return web.json_response(FEEDS[feed_name].get_feed(requester_did, limit, cursor))



async def main():
	app = web.Application()
	app.add_routes([
		web.get("/", hello),
		web.get("/.well-known/did.json", did_doc),
		web.get("/xrpc/app.bsky.feed.getFeedSkeleton", get_feed_skeleton),
	])

	cors = aiohttp_cors.setup(app, defaults={
		"*": aiohttp_cors.ResourceOptions(
			allow_credentials=True,
			expose_headers="*",
			allow_headers="*"
		)
	})

	for route in app.router.routes():
		cors.add(route)
	
	LOG_FMT = '%{X-Forwarded-For}i %t (%Tf) "%r" %s %b "%{Referer}i" "%{User-Agent}i"'
	runner = web.AppRunner(app, access_log_format=LOG_FMT)
	await runner.setup()
	site = web.TCPSite(runner, host=LISTEN_HOST, port=LISTEN_PORT)
	await site.start()

	#while True:
	#	await asyncio.sleep(3600)  # sleep forever

	con = sqlite3.connect("firehose.db")
	cur = con.cursor()
	firehose = FirehoseClient(BGS_HOST, cur)
	async for event in firehose.record_events():
		for feed in FEEDS.values():
			feed.process_event(event)

if __name__ == "__main__":
	asyncio.run(main())
