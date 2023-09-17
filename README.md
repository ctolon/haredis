# haredis: Python Extension for redis with Abstractions

This project is under development. It is not ready for production use.

* `Author`: Cevat Batuhan Tolon
* `Contact`: cevat.batuhan.tolon@cern.ch

Note:
* The special $ ID is only for messages that are added to the stream AFTER you start reading with XREAD.  In your example above, you add the messages to the stream first, then call XREAD. Doing it in the reverse order will work as expected (though you'll need to add the messages from another process or thread).

# stream_name: str = list(streams.keys())[0]
# last_id = await self.get_last_stream_id(stream_name=stream_name)
# lock_key_status = await self.client_conn.get(lock_key)
# print("STREAM:", await self.client_conn.exists(f"stream:{lock_key}"))