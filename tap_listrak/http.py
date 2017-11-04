import zeep
from singer import metrics

WSDL = "https://webservices.listrak.com/v31/IntegrationService.asmx?wsdl"


def get_client(config):
    client = zeep.Client(wsdl=WSDL)
    elem = client.get_element("{http://webservices.listrak.com/v31/}WSUser")
    headers = elem(UserName=config["username"], Password=config["password"])
    client.set_default_soapheaders([headers])
    return client


def request(tap_stream_id, service_fn, **kwargs):
    with metrics.http_request_timer(tap_stream_id) as timer:
        response = service_fn(**kwargs)
        timer.tags[metrics.Tag.http_status_code] = 200
    return response
