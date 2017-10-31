import zeep

WSDL = "https://webservices.listrak.com/v31/IntegrationService.asmx?wsdl"


def get_client(config):
    client = zeep.Client(wsdl=WSDL)
    elem = client.get_element("{http://webservices.listrak.com/v31/}WSUser")
    headers = elem(UserName=config["username"], Password=config["password"])
    client.set_default_soapheaders([headers])
    return client
