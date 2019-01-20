"""
Module for reading, storing and processing officials d2.
"""
__author__ = 'hammer'

from .config import conf
from . import util

import datetime
import pandas as pd
import pygsheets.exceptions as pygerror
from googleapiclient.errors import HttpError


def parse_officials_info(off_wb):
    """
    This takes a OHD and creates a dict of the Official's Profile info for adding into a DataFrame row of all the
    officials in the Register
    :param off_wb: the Google Sheets object
    :return: a dict of all the officials info
    """
    # ws = off_wb.worksheet_by_title('Profile')
    df = util.read_tab_as_df(off_wb, 'Profile')
    offinfo = dict()
    offinfo['ID'] = off_wb.id
    offinfo['Name_Preferred_raw'] = df.iloc[0, 1]
    offinfo['Pronoun_raw'] = df.iloc[1, 1]
    offinfo['Name_Derby'] = df.iloc[2, 1]
    offinfo['Name_Legal'] = df.iloc[3, 1]
    offinfo['Location_raw'] = df.iloc[4, 1]
    offinfo['Affiliated_League_raw'] = df.iloc[5, 1]
    offinfo['Cert_Ref_raw'] = df.iloc[6, 1]
    offinfo['Endorsements_Ref_raw'] = df.iloc[7, 1]
    offinfo['Cert_NSO_raw'] = df.iloc[8, 1]
    offinfo['Endorsements_NSO_raw'] = df.iloc[9, 1]
    offinfo['Officiating_Number'] = df.iloc[2, 3]
    offinfo['Email_Address'] = df.iloc[3, 3]
    offinfo['Phone_Number'] = df.iloc[4, 3]
    offinfo['Insurance_Derby'] = df.iloc[5, 3]
    offinfo['Association_Affiliations_raw'] = df.iloc[6, 3]

    return offinfo


def load_history(doc_id: str):
    """
    Load a single history doc from the Google Doc ID, and returns a tuple of DataFrames (officials information, game data)
    :param doc_id: the Google Sheets ID
    :return: a tuple of DataFrames (officials information, game data)
    """
    start = datetime.datetime.now()
    conf.logger.debug(f"Starting to load the history data")

    officials = pd.DataFrame(columns=conf.runtime.history_officials_data_list)
    games = pd.DataFrame(columns=conf.runtime.history_tab_list)
    # games = pd.DataFrame()

    client = conf.google.client
    if not client:
        client = util.authenticate_with_google()

    last_checkpoint = datetime.datetime.now()
    # TODO load the cache (probably needs a helper function, included in the above) - possibly in read_tab_as_df()
    conf.logger.debug(f"Attempting to load Official ID {doc_id}")
    try:
        off_wb = client.open_by_key(doc_id)
        # TODO: fail on bad version

        officials.loc[doc_id] = parse_officials_info(off_wb)

        # TODO: make this a function:
        off_gh = util.read_tab_as_df(off_wb, 'Game History', num_columns=len(conf.runtime.history_tab_list))

        # change the datatype of Date to be a date, and make the Date the index
        if 'Date' not in off_gh.columns:
            raise Exception(f"Couldn't load the Games History tab for {doc_id}.")
        off_gh['Date'] = pd.to_datetime(off_gh['Date'])
        off_gh = off_gh.set_index('Date')

        if not off_gh.empty:
            conf.logger.debug(f"ID = {doc_id}, shape = {off_gh.shape}")
            games = off_gh
        else:
            conf.logger.warning(f"Can't add empty game history for {doc_id}.")
        time_to_load = datetime.datetime.now() - last_checkpoint
        conf.google.runtime_api.append(time_to_load)
        # TODO: fix up the exceptions
    except pygerror.WorksheetNotFound:
        conf.logger.error(f"Worksheet was not found in {doc_id}")
    except ValueError as e:
        conf.logger.error(f"Mismatched value errors on {doc_id}, skipping it")
        # TODO: figure out this error and fix it
    except HttpError as e:
        if e.resp['status'] in ['404']:
            conf.logger.warning(f"Could not load document {doc_id} because of known HTTP error {e}")
        else:
            conf.logger.warning(f"Could not load document {doc_id} because of unknown HTTP error {e}")
    except Exception as e:
        conf.logger.warning(f"Could not load document {doc_id} because of {e}, GH tab = {off_gh}")

    conf.logger.info(f"Finished {__name__} in {(datetime.datetime.now() - start).total_seconds():.2f}s")
    return officials, games
