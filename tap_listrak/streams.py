from collections import namedtuple
from datetime import datetime, timedelta, date, timezone
import pendulum
from zeep.helpers import serialize_object
import singer
from singer.utils import strftime
from . import schemas
from .schemas import IDS
from .http import request

LOGGER = singer.get_logger()


def gen_intervals(ctx, start_str):
    start_dt = pendulum.parse(start_str)
    interval = timedelta(days=ctx.config.get("interval_days", 365))
    while start_dt < ctx.now:
        end_dt = min(start_dt + interval, ctx.now)
        yield start_dt, end_dt
        start_dt = end_dt


def gen_pages():
    page = 1
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


def add_list_id(lst, records):
    for record in records:
        record["ListID"] = lst["ListID"]
    return records


def add_msg_id(msg, records):
    for record in records:
        record["MsgID"] = msg["MsgID"]
    return records


class BOOK(object):
    SUBSCRIBED_CONTACTS = [IDS.SUBSCRIBED_CONTACTS, "AdditionDate"]
    MESSAGE_CLICKS = [IDS.MESSAGE_CLICKS, "ClickDate"]
    MESSAGE_UNSUBS = [IDS.MESSAGE_UNSUBS, "RemovalDate"]
    MESSAGE_BOUNCES = [IDS.MESSAGE_UNSUBS, "BounceDate"]
    MESSAGE_OPENS = [IDS.MESSAGE_OPENS, "OpenDate"]
    MESSAGE_READS = [IDS.MESSAGE_READS, "ReadDate"]
    MESSAGE_SENDS = [IDS.MESSAGE_SENDS, "SendDate"]


def sync_subscribed_contacts(ctx, lists):
    start_dt = ctx.update_start_date_bookmark(BOOK.SUBSCRIBED_CONTACTS)
    for lst in lists:
        for page in gen_pages():
            response = request(IDS.SUBSCRIBED_CONTACTS,
                               ctx.client.service.ReportRangeSubscribedContacts,
                               ListID=lst["ListID"],
                               StartDate=start_dt,
                               EndDate=ctx.now,
                               Page=page)
            if not response:
                break
            contacts = add_list_id(lst, transform(response))
            write_records(IDS.SUBSCRIBED_CONTACTS, contacts)
    ctx.set_bookmark(BOOK.SUBSCRIBED_CONTACTS, ctx.now)
    ctx.write_state()

SubStream = namedtuple("SubStream", ("tap_stream_id", "bookmark", "endpoint"))
MESSAGE_SUB_STREAMS = [
    SubStream(IDS.MESSAGE_CLICKS, BOOK.MESSAGE_CLICKS, "ReportRangeMessageContactClick"),
    SubStream(IDS.MESSAGE_OPENS, BOOK.MESSAGE_OPENS, "ReportRangeMessageContactOpen"),
    SubStream(IDS.MESSAGE_READS, BOOK.MESSAGE_READS, "ReportRangeMessageContactRead"),
    SubStream(IDS.MESSAGE_UNSUBS, BOOK.MESSAGE_UNSUBS, "ReportRangeMessageContactRemoval"),
    SubStream(IDS.MESSAGE_BOUNCES, BOOK.MESSAGE_BOUNCES, "ReportRangeMessageContactBounces"),
]


def sync_message_sub_stream(ctx, messages, sub_stream):
    start_dt = ctx.update_start_date_bookmark(sub_stream.bookmark)
    for msg in messages:
        for page in gen_pages():
            response = request(sub_stream.tap_stream_id,
                               getattr(ctx.client.service, sub_stream.endpoint),
                               MsgID=msg["MsgID"],
                               StartDate=start_dt,
                               EndDate=ctx.now,
                               Page=page)
            if not response:
                break
            records = add_msg_id(msg, transform(response))
            write_records(sub_stream.tap_stream_id, records)


def sync_sub_streams(ctx, messages):
    for sub_stream in MESSAGE_SUB_STREAMS:
        if sub_stream.tap_stream_id in ctx.selected_stream_ids:
            sync_message_sub_stream(ctx, messages, sub_stream)


def sync_message_sends_if_selected(ctx, messages):
    if not IDS.MESSAGE_SENDS in ctx.selected_stream_ids:
        return
    start_dt = ctx.update_start_date_bookmark(BOOK.MESSAGE_SENDS)
    for msg in messages:
        if pendulum.parse(msg["SendDate"]) < start_dt:
            continue
        for page in gen_pages():
            response = request(IDS.MESSAGE_SENDS,
                               ctx.client.service.ReportMessageContactSent,
                               MsgID=msg["MsgID"],
                               Page=page)
            sent_result = response["ReportMessageContactSentResult"]
            if not sent_result:
                break
            records = add_msg_id(msg, transform(sent_result["WSMessageRecipient"]))
            write_records(IDS.MESSAGE_SENDS, records)


def update_sub_stream_bookmarks(ctx):
    for sub_stream in MESSAGE_SUB_STREAMS:
        if sub_stream.tap_stream_id in ctx.selected_stream_ids:
            ctx.set_bookmark(sub_stream.bookmark, ctx.now)


def update_message_sends_bookmark(ctx, max_send_dt):
    if IDS.MESSAGE_SENDS in ctx.selected_stream_ids and max_send_dt:
        ctx.set_bookmark(BOOK.MESSAGE_SENDS, max_send_dt)


def new_max_send_dt(messages, old_max):
    max_this_batch = max(m["SendDate"] for m in messages)
    return max(max_this_batch, old_max) if old_max else max_this_batch


def sync_messages(ctx, lists):
    start_dt = ctx.config["start_date"]
    max_send_dt = None
    for lst in lists:
        for begin_dt, end_dt in gen_intervals(ctx, start_dt):
            response = request(IDS.MESSAGES,
                               ctx.client.service.ReportListMessageActivity,
                               ListID=lst["ListID"],
                               StartDate=begin_dt,
                               EndDate=end_dt,
                               IncludeTestMessages=True)
            act_result = response["ReportListMessageActivityResult"]
            if not act_result:
                continue
            messages = transform(act_result["WSMessageActivity"])
            write_records(IDS.MESSAGES, messages)
            max_send_dt = new_max_send_dt(messages, max_send_dt)
            sync_sub_streams(ctx, messages)
            sync_message_sends_if_selected(ctx, messages)
    update_sub_stream_bookmarks(ctx)
    update_message_sends_bookmark(ctx, max_send_dt)
    ctx.write_state()


def sync_lists(ctx):
    response = request(IDS.LISTS, ctx.client.service.GetContactListCollection)
    lists = transform(response)
    write_records(IDS.LISTS, lists)
    if IDS.MESSAGES in ctx.selected_stream_ids:
        sync_messages(ctx, lists)
    if IDS.SUBSCRIBED_CONTACTS in ctx.selected_stream_ids:
        sync_subscribed_contacts(ctx, lists)
