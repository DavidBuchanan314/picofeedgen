import dag_cbor
from multiformats import CID
#import hashlib

# LEB128
def parse_varint(stream):
	n = 0
	shift = 0
	while True:
		val = stream.read(1)[0]
		n |= (val & 0x7f) << shift
		if not val & 0x80:
			return n
		shift += 7

def enumerate_and_verify_car_blocks(car):
	while True:
		start = car.tell()
		try:
			block_len = parse_varint(car)
		except IndexError:
			assert(start == car.tell()) # make sure we didn't EOF mid-varint
			return
		cid_raw = car.read(36) # XXX: this needs to be parsed properly, length might not be 36
		cid = CID.decode(cid_raw)
		block_data = car.read(block_len-36)
		#content_hash = hashlib.sha256(block_data).digest()
		#assert(cid_raw.endswith(content_hash))
		yield cid, block_data


def enumerate_car(car):
	header_len = parse_varint(car)
	car_header = dag_cbor.decode(car.read(header_len))
	assert(car_header.get("version") == 1)
	assert(len(car_header.get("roots", [])) == 1)
	return car_header["roots"][0], enumerate_and_verify_car_blocks(car)
