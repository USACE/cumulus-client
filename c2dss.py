#!/usr/env python3
import argparse
from datetime import datetime
import json
import logging
from multiprocessing import Pool
import os
from pytz import utc
import sys
import subprocess
from timeit import default_timer as timer
import tempfile
from uuid import uuid4

# Handled this way for embedded python
sys.path.append(os.path.abspath('./'))
from config import CONFIG
import dss_helpers as dss
import cumulus_api as api
from helpers import printProgressBar


def warp_to_vrt(url, outfile, t_srs='EPSG:5070', algorithm="bilinear", extra_args=None):

    logging.info('gdalwarp;\n  infile: {};\n  outfile: {}'.format(url, outfile))

    cmd = [
        'gdalwarp',
        '-of', 'vrt',
        '-r', algorithm,
        '-t_srs', t_srs
    ]

    if extra_args is not None:
        cmd += extra_args

    # Add input and output files to the command
    cmd += ['/vsicurl/{}'.format(url), outfile]

    # Convert all command arguments to strings. Coordinates, bounding box values,
    # are often int/float values which are not allowed in subprocess commands
    cmd = [str(c) for c in cmd]

    logging.debug('run command: {}'.format(' '.join(cmd)))

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, err = p.communicate()
    logging.debug('SubprocessResults: {}'.format(out))

    return outfile


def translate_to_ascii(infile, outfile, extra_args=None):

    logging.info('gdal_translate;\n  infile: {};\n  outfile: {}'.format(infile, outfile))

    cmd = [
        'gdal_translate',
        '-of', 'AAIGrid',
    ]

    if extra_args is not None:
        cmd += extra_args
    
    cmd += [infile, outfile]

    logging.debug('run command: {}'.format(' '.join(cmd)))

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, err = p.communicate()
    logging.debug('SubprocessResults: {}'.format(out))

    return outfile


def convert_ascii_to_dss(infile, outfile, pathname, gridtype, dtype, dunits, extra_args=[], exe=CONFIG['ASC2DSSGRID']):

    logging.info(f'asc2dssGrid;\n  infile: {infile};\n  outfile: {pathname}')

    cmd = [exe, 'INPUT={}'.format(infile), ]

    cmd += ['DSSFILE={}'.format(outfile), 'PATHNAME={}'.format(pathname), 'GRIDTYPE={}'.format(gridtype),
            'DTYPE={}'.format(dtype), 'DUNITS={}'.format(dunits), ] + extra_args

    logging.debug('run command: {}'.format(' '.join(cmd)))

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, err = p.communicate()
    logging.debug('SubprocessResults: {}'.format(out))


def process_productfile_for_basin(productfile, product, basin, config, outfile):

    with tempfile.TemporaryDirectory() as d:

        # Keep track of runtime statistics
        runstats = {
            'product': product['name'],
            'productfile': productfile['id'],
            'basin': basin['name'],
            'download_start': timer()
        }

        # This is a throw-away file. The ascii (AAIGrid) driver for GDAL can't write directly with gdalwarp
        _file = productfile["file"]

        local_vrt = warp_to_vrt(
            _file,
            os.path.join(d, f'{productfile["id"]}__{basin["id"]}.vrt'),
            t_srs='EPSG:5070',
            algorithm="bilinear",
            extra_args=['-te', basin['x_min'], basin['y_min'], basin['x_max'], basin['y_max'],
                        '-tap', '-tr', '2000', '2000'],  # '--config', 'GDAL_HTTP_UNSAFESSL', 'YES']
        )

        # More runtime statistics
        runstats['download_finish'] = timer()
        runstats['dss_start'] = timer()

        # Convert to ASCII
        ascii_file = translate_to_ascii(
            local_vrt,
            os.path.join(d, f'{productfile["id"]}__{basin["id"]}.asc'),
        )
        
        # Convert to DSS; if outfile is a directory, write "CWMS MODE" DSS files, param.yyyy.mm.dss
        extra_args = []
        if os.path.isdir(outfile):
            extra_args = [f'CWMSDIR={outfile}', f'PARAMETER={dss.get_parametercategory(product)}']

        convert_ascii_to_dss(
            ascii_file,                                     # Infile
            outfile,                                        # Outfile (can override by passing CWMSDIR= and PARAMETER=)
            dss.get_pathname(basin, product, productfile),  # /a/b/c/d/e/f/
            dss.get_crsname(5070),                          # SHG or HRAP
            dss.get_datatype(product),                      # INST-VAL or PER-CUM
            dss.get_dunit(product),                         # MM, etc.
            # cwms directory (for param.yyyy.mm.dss files), this overrides parameter "outfile"
            extra_args=extra_args
        )

    runstats['dss_finish'] = timer()
    # Some summary stats
    # Time spent in download, but includes gdalwarp (reprojection and resampling is part of download)
    runstats['download time (includes reprojection and resampling)'] = runstats['download_finish'] - \
        runstats['download_start']
    # Time spent in format conversion (--> ascii --> dss )
    runstats['convert to DSS time'] = runstats['dss_finish'] - runstats['dss_start']
    runstats['time'] = runstats['dss_finish'] - runstats['download_start']

    return runstats


def mp_process_productfile_for_basin(args):
    """A wrapper to convert args array passed via pool.map() to avoid tons of dictionary lookups in process_productfile_for_basin()"""
    stats = process_productfile_for_basin(args["productfile"], args["product"], args["basin"], args["config"], args["outfile"])
    return stats


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--outfile', required=True, help="Filepath to DSS output file, e.g. C:")
    parser.add_argument('--basins', required=True, nargs="+", help="Basin ID(s) to be processed")
    parser.add_argument('--products', required=True, nargs="+", help="Product ID(s) to be processed")
    parser.add_argument('--datetime-after', required=True,
                        help='Date string in format YYYYMMDD-HHMM (example: 20190101-0000); Assumes UTC')
    parser.add_argument('--datetime-before', required=True,
                        help='Date string in format YYYYMMDD-HHMM (example: 20190102-0000); Assumes UTC')
    parser.add_argument('--parallel', action="store_true",
                        help='Use multiprocessing to boost speed and computer fan noise'),
    parser.add_argument('--processes', type=int, default=4, help='Number of multiprocessing processes')

    args = parser.parse_args()

    print(f'Running c2dss.py with arguments: {args}')

    try:
        # Force timezone to UTC to avoid unaware datetime
        dt_after = datetime.strptime(args.datetime_after, '%Y%m%d-%H%M%S').replace(tzinfo=utc)
        # Force timezone to UTC to avoid unaware datetime
        dt_before = datetime.strptime(args.datetime_before, '%Y%m%d-%H%M%S').replace(tzinfo=utc)
    except:
        logging.critical('Could not parse supplied datetime arguments')
        exit(1)

    # Logging configuration
    logging.basicConfig(level=CONFIG['LOGLEVEL'], format='%(asctime)s; %(levelname)s; %(message)s')

    # Output file
    outfile = os.path.abspath(args.outfile)

    # Basins for processing
    basin_ids = args.basins
    basins = {
        b: api.get_basin(b) for b in basin_ids
    }

    # Dictionary of Product_ID: Product Object
    product_ids = args.products
    products = {
        p: api.get_product(p) for p in product_ids
    }

    # Dictionary of Product_ID: ProductFiles
    productfiles = {
        p: api.get_productfiles(p, datetime_after=dt_after, datetime_before=dt_before) for p in product_ids
    }

    # Log a helpful message
    # logging.info(f'Processing:\n  Basins: {" ".join([b["name"] for b in basins.values()])}\n  Products: {" ".join([p["name"] for p in products.values()])}')
    logging.info('\n\n  Outfile:  {}\n  Processing Dates:  {}\n  Basins:\n    {}\n  Products:\n    {}\n'.format(
        outfile,
        f"{dt_after} -- to -- {dt_before}",
        "\n    ".join([f'{b["id"]} :  {b["name"]}' for b in basins.values()]),
        "\n    ".join([f'{p["id"]} :  {p["name"]}' for p in products.values()]),
    ))
    
    # Create a dictionary to hold runtime statistics
    runstats = {'start': timer()}

    tasks = []
    for product_id, productfiles in productfiles.items():
        for _productfile in productfiles:
            for _basin_id, _basin in basins.items():
                tasks.append(
                    {'productfile': _productfile, 'product': products[product_id], 'basin': _basin, 'config': CONFIG, 'outfile': outfile}
                )

    # Process all tasks
    num_tasks, results = len(tasks), []
    # Run in parallel
    if args.parallel:
        # https://stackoverflow.com/questions/5666576/show-the-progress-of-a-python-multiprocessing-pool-map-call
        for i, result in enumerate(
            Pool(processes=args.processes).imap(mp_process_productfile_for_basin, tasks)
        ):
            results.append(result)
            printProgressBar(i+1, num_tasks, prefix="  Progress: ", suffix='Complete', length=50)
    else:
        i = 0
        for t in tasks:
            result = mp_process_productfile_for_basin(t)
            results.append(t)
            printProgressBar(i+1, num_tasks, prefix="Progress", suffix='Complete', length=50)
            i += 1
    # Add finish time to runtime statistics
    runstats['finish'] = timer()
    runstats['time_total'] = runstats['finish'] - runstats['start']

    print("  Processing Complete: {} seconds".format(runstats["time_total"]))

    # Dump stats to logfile
    if CONFIG['SAVE_LOGS']:
        # Save log in the same directory as output file
        _log = os.path.join(
            os.path.dirname(outfile),
            f'c2dss_runlog_{datetime.now().strftime("%Y-%m-%d-%H%M%S")}.log'
        )
        with open(_log, 'w') as logfile:
            # https://treyhunner.com/2016/02/how-to-merge-dictionaries-in-python/
            logfile.write(
                json.dumps({
                    'parallel': args.parallel,
                    'processes': args.processes,
                    **runstats,
                    'tasks': [_r for _r in results],
                }, indent=3)
            )
