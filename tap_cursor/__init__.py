import os
import json
import collections
import requests
import singer
import argparse
from singer import bookmarks, metrics, metadata

session = requests.Session()
logger = singer.get_logger()

BASE_URL: str = "https://api.cursor.com"

# set default timeout of 300 seconds
REQUEST_TIMEOUT = 300

REQUIRED_CONFIG_KEYS = ["start_date"]

KEY_PROPERTIES = {
    "daily_usage": ["date", "email"],
}

SUB_STREAMS = {}

class DependencyException(Exception):
    pass

class CursorException(Exception):
    pass

class BadCredentialsException(CursorException):
    pass

def translate_state(state, catalog):
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        if bookmarks.get_bookmark(state, stream_name, "since"):
            new_state["bookmarks"][stream_name]["since"] = (
                bookmarks.get_bookmark(state, stream_name, "since")
            )

    return new_state

def get_bookmark(state, stream_name, bookmark_key, start_date):
    stream_dict = bookmarks.get_bookmark(state, stream_name, bookmark_key)
    if stream_dict:
        return stream_dict
    if start_date:
        return start_date
    return None

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = (
        "Unable to extract '{0}' data, "
        "to receive '{0}' data, you also need to select '{1}'."
    )

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(mdata, values, breadcrumb):
    mdata.append({"metadata": values, "breadcrumb": breadcrumb})

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    # mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(
        mdata, (), "table-key-properties", KEY_PROPERTIES[schema_name]
    )

    for field_name in schema["properties"].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "automatic"
            )
        else:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "available"
            )

    return mdata

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path, encoding="utf-8") as file:
            schemas[file_raw] = json.load(file)

    return schemas

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


# return the 'timeout'
def get_request_timeout():
    args = singer.utils.parse_args([])
    # get the value of request timeout from config
    config_request_timeout = args.config.get("request_timeout")

    # only return the timeout value if it is passed in the config and the value is not 0, "0" or ""
    if config_request_timeout and float(config_request_timeout):
        # return the timeout from config
        return float(config_request_timeout)

    # return default timeout
    return REQUEST_TIMEOUT


def authed_get(source, url, headers={}, start_date=0):
    with metrics.http_request_timer(source) as timer:
        epoch_time = int(start_date.timestamp() * 1000)
        session.headers.update(headers)
        logger.info("Making request to %s", url)
        resp = session.request(method="post", json={"startDate": epoch_time}, url=url, timeout=get_request_timeout())
        logger.info("Request received status code %s", resp.status_code)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        if resp.status_code in [404, 409]:
            # return an empty response body since we're not raising a NotFoundException
            resp._content = b"{}"  # pylint: disable=protected-access
        return resp


def authed_get_all_pages(source, url, headers={}, start_date=0):
    while True:
        r = authed_get(source, url, headers, start_date)
        yield r
        if "next" in r.links:
            url = r.links["next"]["url"]
        else:
            break

def get_daily_usage(schema, state, mdata, start_date):
    bookmark_value = get_bookmark(
        state, "daily_usage", "since", start_date
    )
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        bookmark_time = 0

    with metrics.record_counter(
        "daily_usage",
    ) as counter:
        for response in authed_get_all_pages(
            "daily_usage",
            f"{BASE_URL}/teams/daily-usage-data",
            start_date=bookmark_time,
        ):
            daily_usages = response.json()["data"]
            extraction_time = singer.utils.now()
            for daily_usage in daily_usages:
                try:
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(
                            daily_usage,
                            schema,
                            metadata=metadata.to_map(mdata),
                        )
                except:
                    logger.exception(f"Failed to transform record [{daily_usage}]")
                    raise
                singer.write_record(
                    "daily_usage", rec, time_extracted=extraction_time
                )
                singer.write_bookmark(
                    state,
                    "daily_usage",
                    "since",
                    singer.utils.strftime(extraction_time),
                )
                counter.increment()
        return state


def do_discover(config):
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_selected_streams(catalog):
    """
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    """
    selected_streams = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams

def do_sync(config, state, catalog):
    api_key = config["api_key"]
    # Bitbucket only uses Bearer token authentication
    session.headers.update({"Authorization": "Bearer " + api_key})

    start_date = config["start_date"] if "start_date" in config else None
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    state = translate_state(state, catalog)
    singer.write_state(state)

    # pylint: disable=too-many-nested-blocks
    for stream in catalog["streams"]:
        stream_id = stream["tap_stream_id"]
        stream_schema = stream["schema"]
        mdata = stream["metadata"]
        # if it is a "sub_stream", it will be sync'd by its parent
        if not SYNC_FUNCTIONS.get(stream_id):
            continue
        # if stream is selected, write schema and sync
        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])
            # get sync function and any sub streams
            sync_func = SYNC_FUNCTIONS[stream_id]
            sub_stream_ids = SUB_STREAMS.get(stream_id, None)
            # sync stream
            if not sub_stream_ids:
                state = sync_func(stream_schema, state, mdata, start_date)
            # handle streams with sub streams
            else:
                stream_schemas = {stream_id: stream_schema}
                stream_mdata = {stream_id: mdata}
                # get and write selected sub stream schemas
                for sub_stream_id in sub_stream_ids:
                    if sub_stream_id in selected_stream_ids:
                        sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                        stream_schemas[sub_stream_id] = sub_stream["schema"]
                        stream_mdata[sub_stream_id] = sub_stream["metadata"]
                        singer.write_schema(
                            sub_stream_id,
                            sub_stream["schema"],
                            sub_stream["key_properties"],
                        )
                # sync stream and it's sub streams
                state = sync_func(
                    stream_schemas, state, stream_mdata, start_date
                )
            singer.write_state(state)


SYNC_FUNCTIONS = {
    "daily_usage": get_daily_usage,
}

@singer.utils.handle_top_exception(logger)
def main():
    global config_path

    # Store config path for later use
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.json")
    path_args, unknown = parser.parse_known_args()
    config_path = path_args.config

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if not args.config.get("api_key"):
        if os.getenv("CURSOR_API_KEY") is not None:
            args.config["api_key"] = os.getenv("CURSOR_API_KEY")
        else:
            raise BadCredentialsException(
                "No API key provided."
            )
    
    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()

        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()