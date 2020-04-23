import datetime
import os
import sys

sys.path.append(os.path.abspath('./'))
import cumulus_api as api

# Copy of Django's djago.utils.dateparse
from django_dateparse import parse_duration, parse_datetime

def get_crsname(EPSG):

    dss_crsnames = {
        '5070': 'SHG',
    }

    return dss_crsnames[str(EPSG)]


def dpart(dt, temporal_duration):
    """Helper function to return correctly formatted DSS d_part string"""

    if parse_duration(temporal_duration) == datetime.timedelta(0):
        d = parse_datetime(dt)
    else:
        d = parse_datetime(dt) - parse_duration(temporal_duration)
    
    return d.strftime('%d%b%Y:%H%M').upper()


def epart(dt, temporal_duration):
    """Helper function to return correctly formatted DSS e_part string"""
    
    if parse_duration(temporal_duration) == datetime.timedelta(0):
        return ""
    else:
        return parse_datetime(dt).strftime('%d%b%Y:%H%M').upper()


def get_pathname_parts(basin, product, productfile):
    """Craft a DSS pathname from a basin (watershed), product metadata, and productfile
    Basin is required for DSS a_part (watershed name)
    Product is required for DSS f_part and duration information
    Productfile is required for the timestamp, used in d_part, e_part.
    """

    return {
        'a_part': 'SHG',
        'b_part': basin['name'],
        'c_part': product['parameter'],
        'd_part': dpart(productfile['datetime'], product['temporal_duration']),
        'e_part': epart(productfile['datetime'], product['temporal_duration']),
        'f_part': product['dss_fpart'],
    }


def get_pathname(basin, product, productfile):
    
    parts = get_pathname_parts(basin, product, productfile)

    return f'/{"/".join(parts.values())}/'


def get_dunit(product):
    
    return product['unit']


def get_datatype(product):

    if parse_duration(product['temporal_duration']) == datetime.timedelta(0):
        return 'INST-VAL'
    else:
        return 'PER-CUM'


def get_parametercategory(product):
    """Helper function to determine string prefix for monthly DSS files (<Snow|Airtemp|Precip>.YYYY.MM.dss)"""
    
    parameters = { _p["name"]: _p["parametercategory"] for _p in api.get_parameters() }
    
    p = parameters[product["parameter"]]

    return f'{p}.'