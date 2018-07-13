"""Open the History Register, then load all the history docs listed in the Register."""

__author__ = 'hammer'

import gspread
import datetime
import time
# OHD specific imports
import util
import official
import config


def load_register(doc_id='1vrvYjcNLTdWypfeWjMUyMuo3LVtX80CKMRY-3SegdDw', tab_name='History Register', id_col_num=4,
                  id_col_header=True, cred_file=None):
    """
    Opens a History Register google document, with a specified Google Sheet ID, and loads the doc IDs from the specified
    column. By default this will load the central OffCom History Register.
    :param doc_id: Google Sheet ID of the History Register
    :param tab_name: Google Sheet ID of the History Register
    :param id_col_num: The column in the Register that contains the Google Sheet IDs of the individual history docs
    :param id_col_header: Does the column in the Register that contains the Google Sheet IDs have a header row?
    :param cred_file: The location of JSON file with the service account credentials in it
    :return: A list of loaded history doc IDs from the Register
    """
    # process default arguments
    if cred_file is None:
        cred_file = config.cred_file

    # start the meat of the function
    start = datetime.datetime.now()
    r = util.GoogleSheet(doc_id, cred_file=cred_file)
    rdata = r.get_tab_data(tab_name, array_format=True)

    # authenticate with google and open their geocoding service
    # google_api = util.connect_to_geocode_api()

    # Collect the list of Google Sheet IDs for the officials in the Register
    # remove header row, if there is one:
    if id_col_header:
        doc_ids = list(rdata[1:, id_col_num])
    else:
        doc_ids = list(rdata[:, id_col_num])
    doc_ids = filter(None, doc_ids)  # remove blank entries from the list
    print u'Getting list of {} Register IDs took {}'.format(len(doc_ids), datetime.datetime.now() - start)

    return doc_ids


def load_official(history):
    """
    Takes a single OHD doc connection and loads the Official. Returns the successfully loaded Official.
    :param history: the GoogleSheet object for the OHD to load
    :return: The populated Official object
    """
    start = datetime.datetime.now()

    # Go through the Google Sheet and load the data from it, tab at a time.
    meta_values = history.get_tab_data('Metadata')
    tmpl_ver = meta_values[3][0]
    most_recent_game = meta_values[5][0]
    last_changed = meta_values[7][0]
    profile_values = history.get_tab_data('Profile')
    [pref_name, derby_name, legal_name] = util.get_names(profile_values)
    print(u'Off: {}, ver {}, most recent game {}, setting of "now" in their metadata tab: {}'.format(pref_name, tmpl_ver, most_recent_game, last_changed))
    off = official.Official(history.doc_id)
    off.pref_name = unicode(pref_name)
    off.derby_name = unicode(derby_name)
    off.legal_name = unicode(legal_name)
    off.officiating_number = profile_values[3][3]
    off.refcert = util.normalize_cert(profile_values[7][1])
    off.nsocert = util.normalize_cert(profile_values[9][1])
    # TODO: cert endorsements
    # off.refcert_endorsements = util.normalize_endorsements(profile_values[8][1])
    # off.nsocert_endorsements = util.normalize_endorsements(profile_values[10][1])
    # TODO: normalize pronouns
    off.pronouns = profile_values[2][1]
    off.league_affiliation = profile_values[6][1]
    # TODO: geo location magic goes here
    off.location = profile_values[5][1]
    # TODO: This is offline until the geocoding bit is fixed - new product offering
    # off.locationref = util.normalize_officials_location(google_api, off.location, off.league_affiliation)
    # add all the games from the Game History tab, minus the header row
    off.add_history(history.get_tab_data('Game History')[1:])

    # profiling and testing/verification section
    print u'Loading {} took {}'.format(off.pref_name, datetime.datetime.now() - start)

    # return the loaded official
    return off


def ohd_conn_generator(id_list, cred_file=None):
    """
    This ia a Generator that takes a list of OHD doc IDs and iterates through it and yields a GoogleSheet object of
    each OHD, in turn, from the list.
    Returns the list of the attempted OHD updates, and the result of the attempt.
    This will ideally replace the
    :param id_list: list of OHD doc IDs to load
    :param cred_file: The JSON file with the service account credentials in it
    :return: yeilds each live GoogleSheet object in turn
    """
    # process default arguments
    if cred_file is None:
        cred_file = config.cred_file

    # start the meat of the function
    # initializing run/error tracking variables
    err_quota = 0
    err_unavailable = 0
    err_auth = 0

    # Go through the list of Google Sheet IDs and yield each one in turn
    for doc in id_list:
        history = util.GoogleSheet(doc, cred_file)
        # currently this only servers v3 sheets, so skip over any that aren't v3
        if history.version != 3:
            print u'Found a doc with the wrong version: {}'.format(history.doc_id)
            continue

        yield history
        err_quota += history.api['quota_error_count']
        err_unavailable += history.api['unavailable_error_count']
        err_auth += history.api['auth_error_count']

    # summarize the failures encountered
    print u"Found {} Quota errors, {} Resource Unavailable errors and {} Auth Token Expired errors.".format(err_quota, err_unavailable, err_auth)


if __name__ == '__main__':
    start = datetime.datetime.now()
    # get the location info
    # locations = util.load_locations(cred_file='../service-account.json')

    # doc_id = '1vrvYjcNLTdWypfeWjMUyMuo3LVtX80CKMRY-3SegdDw'  # production register
    doc_id = '1mH2Sui25qqrGt0x8k_KuL1BONsZ3rOPCIurECnX3Jxw'  # real test register
    tab_name = 'Test History Register'
    # tab_name = 'History Register'
    config.cred_file = '../service-account.json'
    config.google_api_delay = 1

    olist = load_register(doc_id=doc_id, tab_name=tab_name)
    o = list()
    for off in ohd_conn_generator(olist):
        otemp = load_official(off)
        if otemp:
            o.append(otemp)
        else:
            print(u"This document wasn't loaded {}".format(off.doc_id))

    print u'Officials loaded, there are {} in the list, for a total of {} games'.format(len(o), reduce(lambda x, y: x+y, [len(x.games) for x in o]))
    print u'Officials loaded, there are {} in the list, {}% with game errors'.format(len(o), round(reduce(lambda x, y: x+y, [1 for x in o if x.games_with_errors_count > 0])/len(o)*100.0, 0))
    step = datetime.datetime.now()
    print u'Full loading took {}'.format(step - start)
    # query_string = [{'standard': [True], 'assn': ['WFTDA', 'MRDA']},
    #                 {'type': ['Other'], 'role': ['THR', 'ATHR', 'THSNO', 'ATHNSO']},
    #                 {"start": start.date(), "interval": 12, "max_interval": 5}]
    # for off in o:
    #     qgames = util.query_history(off.positions, *query_string)
    #     if len(qgames[0]) > 0:
    #         print u'{}\'s breakdown of {} is {}'.format(off.pref_name, qgames[0][0].__class__, map(len, qgames))
    #     else:
    #         print u'{} has an empty games history'.format(off.pref_name)
    # print u'Processing took an extra {}'.format(datetime.datetime.now() - step)
