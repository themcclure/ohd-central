"""A collection of utility functions relating to the Officiating History Document."""

__author__ = "hammer"


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re


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
        elif 'Last Revised 2015' in sheet.worksheet('Instructions').acell('A104').value:
            return 2
        elif 'Last Revised 2016' in sheet.worksheet('Instructions').acell('A104').value:
            return 2
        elif 'Last Revised 2017-01-05' in sheet.worksheet('Instructions').acell('A102').value:
            return 2
        else:
            return None
    else:
        return None


def normalize_cert(cert_string):
    """
    Takes the cert string from the history, which is a freeform field, and normalizes it to 1-5 or 0 for not certified
    :param cert_string: string taken directly from the history sheet
    :return: 0-5
    """
    if cert_string is None:
        return 0
    # if it's already a number, return an int (if it's < 1 or greater than 5, return None)
    elif isinstance(cert_string, float) or isinstance(cert_string, int):
        if (cert_string < 1) or (cert_string > 5):
            return 0
        else:
            return int(cert_string)
    # if it's a string with numbers in it, return the first one
    numbers = re.findall(r'\d+', cert_string)
    if numbers:
        return int(numbers[0])
    else:
        # there are no numbers in the cell, look for someone spelling out the numbers by finding the word:
        if 'ONE' in cert_string.upper():
            return 1
        elif 'TWO' in cert_string.upper():
            return 2
        elif 'THREE' in cert_string.upper():
            return 3
        elif 'FOUR' in cert_string.upper():
            return 4
        elif 'FIVE' in cert_string.upper():
            return 5
        else:
            # there are no valid numbers in the string
            return 0
