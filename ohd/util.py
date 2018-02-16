"""A collection of utility functions relating to the Officiating History Document."""
__author__ = "hammer"


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
import datetime
from dateutil import relativedelta
from defaultlist import defaultlist


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


def query_history(items, filter_in=None, filter_out=None, filter_date=None):
    """
    Returns a list of items from the supplied list of items (Games, Positions, Events), that meet the criteria in the
    provided filters. All items are matched against the include list first, then any items matching against the exclude
    list are removed.
    Finally, if there is a date filter provided, items will be returned in a list of lists, grouped by items that lie in
    the different intervals
    :param items: list of items (Games, Positions, Events) to filter
    :param filter_in: A dict of { property: list of values } to select if matched
    :param filter_out: A dict of { property: list of values } to remove if matched
    :param filter_date: A dict of date properties to filter or group by:
                        start: start date (datetime.date) - everything more recent that that will be filtered out.
                                If start is not specified, then it will default to today's date.
                        interval: the size (number of months) of each "bucket" from the start date to group results
                                The first interval (start date - interval) is the 0th interval. If there is no interval
                                specified (or it is not an int >= 0) then all items will be returned in a single list
                        max_interval: the max number of intervals to go back, if there is no max_interval specified then
                                (or it is not an int > 0) it will count all the way back.
                                If there is no interval specified, max_interval is ignored.
    :return: a list of objects of type from scope (Games, Positions, Events)
    """
    if filter_in is None:
        filter_in = dict()
    if filter_out is None:
        filter_out = dict()
    if filter_date is None:
        filter_date = dict()
    proto_list = list()
    fdate = datetime.date.today()

    try:
        proto_list = items
        # iterate through each element in filter_in, and select the matching values
        for item in filter_in.keys():
            proto_list = [i for i in proto_list if getattr(i, item) in filter_in[item]]
        # iterate through each element in filter_out, and remove the matching values
        for item in filter_out.keys():
            proto_list = [i for i in proto_list if hasattr(i, item) and getattr(i, item) not in filter_out[item]]
    except Exception as e:
        print u'History query failed. Item Type: {}, Error: {}, Item was {}'.format(type(items[0]), e, i.raw_row)

    try:
        if 'start' in filter_date and isinstance(filter_date['start'], datetime.date):
            # input overrides the default start date
            fdate = filter_date['start']

        if 'interval' not in filter_date or not isinstance(filter_date['interval'], int) or filter_date['interval'] <= 0:
            # there's no interval set, so just return all items that are older than the start date
            proto_list = [i for i in proto_list if i.gdate <= fdate]
        else:
            bucket_list = defaultlist(list)
            # process each item in the list, and group it into interval buckets
            for item in proto_list:
                # skip over the items before the filter date (fdate)
                if item.gdate > fdate:
                    continue
                datediff = relativedelta.relativedelta(fdate, item.gdate)
                bucket = (datediff.years * 12 + datediff.months) / filter_date['interval']
                bucket_list[bucket].append(item)

            max = len(bucket_list)
            if 'max_interval' in filter_date and isinstance(filter_date['max_interval'], int) and filter_date['max_interval'] > 0:
                # there is an interval but there's no max_interval, so return as many intervals as it takes to bucket all the items
                max = filter_date['max_interval']
            proto_list = bucket_list[:max]
    except Exception as e:
        print u'History query failed on date calcs for Item Type: {}, Error: {}'.format(type(items[0]), e)
    return proto_list
