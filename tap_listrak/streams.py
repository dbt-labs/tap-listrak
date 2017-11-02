from datetime import datetime, timedelta, date, timezone
import pendulum
from zeep.helpers import serialize_object
import singer
from singer.utils import strftime
from . import schemas
from .schemas import IDS

LOGGER = singer.get_logger()


def gen_intervals(ctx, start_dt):
    now = datetime.utcnow()
    interval = timedelta(days=ctx.config.get("interval_days", 60))
    while start_dt < now:
        end_dt = min(start_dt + interval, now)
        yield start_dt, end_dt
        start_dt = end_dt


def gen_pages():
    page = 0
    while True:
        yield page
        page += 1


def metrics(tap_stream_id, records):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(records))


def write_records(tap_stream_id, records):
    singer.write_records(tap_stream_id, records)
    metrics(tap_stream_id, records)


def transform_dts(data):
    if isinstance(data, list):
        new = []
        for item in data:
            new.append(transform_dts(item))
        return new
    if isinstance(data, dict):
        new = {}
        for k, v in data.items():
            new[k] = transform_dts(v)
        return new
    if isinstance(data, date):
        new = data.replace(tzinfo=timezone.utc)
        return strftime(new)
    return data


def transform(response):
    return transform_dts(serialize_object(response))


# class SubscribedContacts(Stream):
#     def sync(self, ctx):
#         fn = ctx.client.service.ReportSubscribedContacts
#         for lst in ctx.cache["lists"]:
#             for page in gen_pages():
#                 response = fn(ListID=lst["ListID"], Page=page)
#                 records = response["ReportSubscribedContactsResult"]
#                 if not records:
#                     break
#                 self.write_records(self.transform(ctx, records))


# class MessageClicks(Stream):
#     def sync(self, ctx, messages):
#         for msg in messages:
#             for start_dt, end_dt in gen_intervals(ctx, start_dt):
#                 for page in gen_pages():
#                     pass
#             # ctx.client.service.ReportRangeMessageContactClick(MsgID=msg["MsgID"],


# message_clicks = MessageClicks("message_clicks", [""])


def sync_messages(ctx, lists):
    fn = ctx.client.service.ReportListMessageActivity
    start_dt = pendulum.parse(ctx.config["start_date"])
    for lst in lists:
        for begin_dt, end_dt in gen_intervals(ctx, start_dt):
            response = fn(ListID=lst["ListID"],
                          StartDate=begin_dt,
                          EndDate=end_dt,
                          IncludeTestMessages=True)
            actresult = response["ReportListMessageActivityResult"]
            if not actresult:
                continue
            messages = transform(actresult["WSMessageActivity"])
            write_records(IDS.MESSAGES, messages)
            # message_clicks.sync(ctx, messages)


def sync_lists(ctx):
    schemas.load_and_write_schema(IDS.LISTS)
    response = ctx.client.service.GetContactListCollection()
    lists = transform(response)
    # write_records(IDS.LISTS, lists)
    sync_messages(ctx, lists)
