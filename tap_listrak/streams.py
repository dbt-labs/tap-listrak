from datetime import datetime, timedelta
import singer
from singer import metrics
from singer.transform import transform as tform

LOGGER = singer.get_logger()


class Stream(object):
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields"""
    def __init__(self, tap_stream_id, pk_fields):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields

    def metrics(self, records):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def transform(self, ctx, records):
        ret = []
        for record in records:
            ret.append(tform(record, ctx.schema_dicts[self.tap_stream_id]))
        return ret


class SubscribedContacts(Stream):
    def sync(self, ctx):
        fn = ctx.client.service.ReportSubscribedContacts
        for lst in ctx.cache["lists"]:
            page = 0
            while True:
                response = fn(ListID=lst["ListID"], Page=page)
                records = response["ReportSubscribedContactsResult"]
                if not records:
                    break
                self.write_records(self.transform(ctx, records))
                page += 1


class Messages(Stream):
    def sync(self, ctx):
        fn = ctx.client.service.ReportListMessageActivity
        bookmark = [self.tap_stream_id, "SendDate"]
        start_dt = ctx.update_start_date_bookmark(bookmark)
        end_dt = datetime.utcnow()
        for lst in ctx.cache["lists"]:
            response = fn(ListID=lst["ListID"],
                          StartDate=start_dt,
                          EndDate=end_dt,
                          IncludeTestMessages=True)
            records = response["ReportListMessageActivityResult"]
            if not records:
                continue
            self.write_records(self.transform(ctx, records))



all_streams = [
    SubscribedContacts("subscribed_contacts", []),
    Messages("messages", ["MsgId"]),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
