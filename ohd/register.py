"""
This module contains all the History Register management functions.
"""

__author__ = 'hammer'

from .config import conf
from . import util

import datetime
import pandas as pd
# import pkg_resources as pr
# from pathlib import Path


# TODO: add a load time to each of the doc process requests into config.google.runtime_api or _cache
def load_register(doc_id=None, tab_name=None, force_refresh=False):
    """
    Loads the History Register google document, with a specified Google Sheet ID, and loads the doc IDs from the specified
    column. By default this will load the central OffCom History Register.
    :param doc_id: explicitly set the Google Sheet ID of the History Register
    :param tab_name: explicitly set the Google Sheet tab name of the History Register
    :param force_refresh: if set to True, will force loading from the Google Doc rather than the cache
    :return: A DataFrame of the entire History Register
    """
    start = datetime.datetime.now()
    last_checkpoint = datetime.datetime.now()
    conf.logger.debug(f"Starting to load the {conf.runtime.label} Register")
    # process default arguments
    reg_doc_id = doc_id
    reg_tab = tab_name
    if reg_doc_id is None:
        reg_doc_id = conf.runtime.reg_id
    if reg_tab is None:
        reg_tab = conf.runtime.reg_tab_name
    needs_refresh = force_refresh

    # TODO: turn this into a cache helper function, returns (object from cache, cache_item_stale) tuple
    # load the Register from cache
    cache = conf.caching.cache
    register = cache['register']
    if register.empty:
        needs_refresh = True  # flag for refresh if the cache is not present
        conf.logger.debug("Need to refresh Register because the cache is empty")

    # flag for refresh if the Register cache is too old
    stale_threshold_date = datetime.datetime.now() - datetime.timedelta(days=conf.runtime.stale_days)
    if 'register' not in cache['metadata'].index or cache['metadata'].loc['register']['last_update'] < stale_threshold_date:
        needs_refresh = True
        conf.logger.debug("Need to refresh Register because the cache is too old")

    conf.logger.debug(f"Loading the Register cache took {(datetime.datetime.now() - last_checkpoint).total_seconds():.2f}s")
    last_checkpoint = datetime.datetime.now()

    # if flagged for refresh and if there is a service account credential configured, then load the Register from Google and update the cache
    cred_file = conf.google.cred_file
    if needs_refresh and cred_file is not None and cred_file.exists():
        client = util.authenticate_with_google()  # initialize the API connection to Google Docs
        reg_wb = client.open_by_key(reg_doc_id)
        register = util.read_tab_as_df(reg_wb, reg_tab, num_columns=len(conf.caching.reg_tab_list))
        conf.caching.cache['register'] = register  # update the register cache in-memory
        conf.caching.cache['metadata'] = pd.DataFrame({'last_update': datetime.datetime.now()}, index=['Register'])  # update the metadata cache in-memory
        conf.caching.persist_cache()  # update the in-memory cache on disk
        conf.logger.debug(f"Refreshing Register and saving {len(register)} to {conf.caching.file}")
        time_to_load = datetime.datetime.now() - last_checkpoint
        conf.google.runtime_api.append(time_to_load)
        conf.logger.debug(f"Loading the Register from doc took {time_to_load.total_seconds():.2f}s")
    elif needs_refresh and cred_file is None:
        conf.logger.warning("Need to refresh Register but no Google credential file was available")
    elif needs_refresh and not cred_file.exists():
        conf.logger.warning(f"Need to refresh Register but tge Google credential file does not exist {cred_file}")
    else:
        conf.logger.debug(f"Using the cached Register with {len(register)} items")

    conf.logger.info(f"Finished {__name__} in {(datetime.datetime.now() - start).total_seconds():.2f}s")

    return register
#
#
# def load_histories(id_list: pd.Series):
#     """
#     Loads the history docs for the officials passed in to the function.
#     The docs will be loaded from the cache, if present and current. Any missing officials will be loaded via the API, if
#     there are credentials present, otherwise they will be omitted from the results.
#     A tuple will be returned containing a DataFrame for the officials' general data, and another DataFrame of the combined
#     Game History data (multi-indexed by date and official's ID).
#     :param id_list: an array-like list of OHD Google Doc IDs
#     :return: a tuple: (DataFrame of Officials data, DataFrame of Game History data)
#     """
#     start = datetime.datetime.now()
#     logger.debug(f"Starting to load the history data")
#
#     officials = pd.DataFrame(columns=config.data['history_officials_data_list'])
#     games = pd.DataFrame(columns=config.data['history_tab_list'])
#
#     # TODO load the cache (probably needs a helper function, included in the above) - possibly in read_tab_as_df()
#     client = config.google['client']
#     if not client:
#         client = util.authenticate_with_google()
#
#     last_checkpoint = datetime.datetime.now()
#     for index, item in id_list.items():
#         logger.debug(f"Attempting to load Official ID {item}")
#         try:
#             off_wb = client.open_by_key(item)
#             off_gh = util.read_tab_as_df(off_wb, 'Game History', num_columns=len(config.data['history_tab_list']))
#
#             officials.loc[item] = item
#             if not off_gh.empty:
#                 logger.debug(f"ID = {item}, shape = {off_gh.shape}")
#                 games.loc[item] = off_gh
#             else:
#                 logger.warning(f"Couldn't add game history for {item}. Length = {len(off_gh)} and columns are {off_gh.columns}")
#         except Exception as e:
#             logger.warning(f"Couldn't load document {item} because of {e}")
#             continue
#         time_to_load = datetime.datetime.now() - last_checkpoint
#         config.google['runtime_api'].append(time_to_load)
#
#     logger.info(f"Finished {__name__} in {(datetime.datetime.now() - start).total_seconds():.2f}s")
#     return officials, games
