"""
Load the History Register from the cache.
If there is no cache, or the cache is stale, and the user has permissions, then update the cache from Google.
"""
__author__ = 'hammer'

import pandas as pd
import ohd
import os
import datetime
from pprint import pprint


##########
# Main executable
if __name__ == '__main__':
    start = datetime.datetime.now()
    last_checkpoint = datetime.datetime.now()

    ##########
    # setup the runtime environment
    runtime_env = os.getenv('OHD_RUNTIME', 'ProdTest')
    conf = ohd.config.conf
    conf.init_env(runtime_env, with_keys=True)
    # conf.import_keys()
    conf.logger.info(f"Starting run in the {runtime_env} environment")

    ##########
    # load the register
    conf.logger.debug(f"About to load the {runtime_env} Register")
    reg = ohd.load_register()
    conf.logger.info(f"Loaded {len(reg)} records from the {runtime_env} Register")

    # load each history doc from the Register
    # conf.logger.debug(f"Attempting to load {len(reg)} officials")
    # last_checkpoint = datetime.datetime.now()
    # officials, games = ohd.load_histories(reg['History ID'])
    # conf.logger.debug(f"Loaded {len(officials)} officials in {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}")

    # ohd.official.load_history('3')  # force an error since no google ID exists
    # ohd.official.load_history_doc('1kG9QTdus7LbpZP-3L9fNvwQ0nVpUUXyw7m7hpKSBH-E')  # force an error since we don't have permission to this google ID
    load_threshold = 20
    loaded_from_google = 0
    for did in reg['History ID']:
        o, g, source = ohd.official.load_history_doc(did)
        if source == 'sheet':
            loaded_from_google += 1
            if loaded_from_google > load_threshold:
                break

    ##########
    # Finishing up and logging info
    # ohd.config.tidy()
    conf.logger.info(f"Total runtime {(datetime.datetime.now() - start).total_seconds():.2f}s")
    conf.logger.info(f"Coda:")
    api_loads_count = pd.DataFrame({'api': conf.google.runtime_api})['api'].count()
    cache_loads_count = pd.DataFrame({'cache': conf.google.runtime_cache})['cache'].count()
    conf.logger.info(f"Loaded {api_loads_count + cache_loads_count} documents.")
    if api_loads_count > 0:
        api_loads_avg = pd.DataFrame({'api': conf.google.runtime_api})['api'].mean().total_seconds()
        conf.logger.info(f"API: {api_loads_count} docs, with an average of {api_loads_avg:.2f}s")
    if cache_loads_count > 0:
        cache_loads_avg = pd.DataFrame({'cache': conf.google.runtime_cache})['cache'].mean().total_seconds()
        conf.logger.info(f"Cache: {cache_loads_count} docs, with an average of {cache_loads_avg:.2f}s")
