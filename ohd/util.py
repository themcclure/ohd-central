"""A collection of utility functions relating to the Officiating History Document."""
__author__ = "hammer"

# from . import config
from .config import conf
from pathlib import Path
import pygsheets
# import datetime
import pandas as pd
# import sqlite3


def authenticate_with_google(cred_file=None):
    """
    Authenticate the service account with google and return a credentialed connection.
    Adds the authorized connection to the config object
    :param cred_file: the file containing the Google credentials to use to authenticate
    :return: the authorized connection (in case someone really needs it)
    """
    if cred_file is None:
        cred_file = conf.google.cred_file
    conn = None
    if Path(cred_file).exists():
        conf.logger.debug(f"Authenticating using credentials in {cred_file}")
        conn = pygsheets.authorize(service_file=cred_file)
        conf.google.client = conn
    else:
        conf.logger.debug(f"Provided credentials file doesn't exist: {cred_file}")

    return conn


def read_tab_as_df(workbook, tab_name, num_columns=None, raw=False):
    """
    Read the named tab from the given Google Sheets workbook, and return the tab as a DataFrame that has been trimmed
    to remove empty/blank cells.
    :param workbook: Google pygsheets object
    :param tab_name: name of the tab to load
    :param num_columns: the number of columns to return
    :param raw: if set to True, then return the full tab as is
    :return: a DataFrame
    """
    df = workbook.worksheet_by_title(tab_name).get_as_df()
    if not raw and not df.empty:
        df.replace('', pd.np.nan, inplace=True)
        df.dropna(how='all', inplace=True)
        if num_columns:
            df = df.iloc[:, :num_columns]
        df.fillna('', inplace=True)

    return df
