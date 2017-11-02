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
    interval = timedelta(days=ctx.config.get("interval_days", 60))
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


def sync_message_clicks(ctx, messages):
    start_dt = ctx.update_start_date_bookmark(BOOK.MESSAGE_CLICKS)
    for msg in messages:
        for page in gen_pages():
            response = request(IDS.MESSAGE_CLICKS,
                               ctx.client.service.ReportRangeMessageContactClick,
                               MsgID=msg["MsgID"],
                               StartDate=start_dt,
                               EndDate=ctx.now,
                               Page=page)
            if not response:
                break
            clicks = add_msg_id(msg, transform(response))
            write_records(IDS.MESSAGE_CLICKS, clicks)


def sync_messages(ctx, lists):
    start_dt = ctx.config["start_date"]
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
            if IDS.MESSAGE_CLICKS in ctx.selected_stream_ids:
                sync_message_clicks(ctx, messages)
    if IDS.MESSAGE_CLICKS in ctx.selected_stream_ids:
        ctx.set_bookmark(BOOK.MESSAGE_CLICKS, ctx.now)
    ctx.write_state()


def sync_lists(ctx):
    response = request(IDS.LISTS, ctx.client.service.GetContactListCollection)
    lists = transform(response)
    write_records(IDS.LISTS, lists)
    if IDS.MESSAGES in ctx.selected_stream_ids:
        sync_messages(ctx, lists)
    if IDS.SUBSCRIBED_CONTACTS in ctx.selected_stream_ids:
        sync_subscribed_contacts(ctx, lists)
