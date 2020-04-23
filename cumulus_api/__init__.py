import ssl
import sys
import urllib.parse

from .config import CUMULUS_API
from .helpers import get_url_as_json

def get_basin(id):
    url = f'{CUMULUS_API}/basins/{id}'
    return get_url_as_json(url)


def get_basins():
    url = f'{CUMULUS_API}/basins/'
    return get_url_as_json(url)


def get_products():
    url = f'{CUMULUS_API}/products/'
    return get_url_as_json(url)


def get_product(id):
    url = f'{CUMULUS_API}/products/{id}'
    return get_url_as_json(url)


def get_parameters():
    url = f'{CUMULUS_API}/parameters/'
    return get_url_as_json(url)


def get_parameter(id):
    url = f'{CUMULUS_API}/parameters/{id}'
    return get_url_as_json(url)


def get_productfiles(product_id, datetime_after=None, datetime_before=None):

    # Base URL Parts
    urlparts = urllib.parse.urlparse(f'{CUMULUS_API}/products/{product_id}/files')

    # Build querystring
    querystring_params = []
    if datetime_after is not None:
        querystring_params.append(
            ("after", datetime_after.strftime("%Y-%m-%dT%H:%M:%S"))
        )
    
    if datetime_before is not None:
        querystring_params.append(
            ("before", datetime_before.strftime("%Y-%m-%dT%H:%M:%S"))
        )
    
    urlparts = urlparts._replace(query=urllib.parse.urlencode(querystring_params))

    return get_url_as_json(
        urllib.parse.urlunparse(urlparts)
    )
