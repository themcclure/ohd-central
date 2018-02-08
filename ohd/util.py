"""A collection of utility functions relating to the Officiating History Document."""

__author__ = "hammer"


import gspread
from oauth2client.service_account import ServiceAccountCredentials


def authenticate_with_google(cred_file):
    """
    Authenticate the service account with google and return a credentialed connection.
    :return: Authorized gspread connection
    """
    scope = ['https://spreadsheets.google.com/feeds']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)

    return gspread.authorize(credentials)


def get_names(sheet):
    """
    Takes the Profile tab and extracts the official's set of names.
    :param sheet: the opened Google sheet object
    :return: a list of names
    """
    tab = sheet.worksheet('Profile')
    pref_name = tab.acell('B2').value
    derby_name = tab.acell('B4').value
    legal_name = tab.acell('B5').value
    if not pref_name and not derby_name:
        pref_name = legal_name
        derby_name = legal_name
    elif not pref_name:
        pref_name = derby_name
    elif not derby_name:
        derby_name = legal_name
    return [pref_name, derby_name, legal_name]


def get_version(sheet):
    """
    Check the info tabs and make a determination about which version of the officiating history document is being used.
    Different versions keep information in different places
    :param sheet: the opened Google sheet object
    :return: integer with the version number, or None for unknown version
    """
    tabs = [x.title for x in sheet.worksheets()]

    if 'Learn More' in tabs:
        return 3
    elif 'WFTDA Summary' in tabs:
        return 1
    elif 'Summary' in tabs:
        if 'WFTDA Referee' in tabs or 'WFTDA NSO' in tabs:
            # this is an old history doc but it's been modified to change the WFTDA Summary tab name
            return None
        elif 'Instructions' not in tabs:
            # this is a new history doc it's been modified to delete the instructions tab (a no no)
            return None
        elif sheet.worksheet('Instructions').acell('A1').value == 'Loading...':
            # found one instance where the Instructions tab was showing "loading" - at the moment this will only happen on the new sheets
            return 2
        elif 'Last Revised 2015' in sheet.worksheet('Instructions').acell('A104').value :
            return 2
        elif 'Last Revised 2016' in sheet.worksheet('Instructions').acell('A104').value :
            return 2
        elif 'Last Revised 2017-01-05' in sheet.worksheet('Instructions').acell('A102').value :
            return 2
        else:
            return None
    else:
        return None
