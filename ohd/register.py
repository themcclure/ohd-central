"""Open the History Register, then load all the history docs listed in the Register."""

__author__ = 'hammer'

import gspread
import datetime
import time
# OHD specific imports
import util
import official


# TODO: add two new arguments, max_docs returned and starting_location, in case this ever gets too big
# TODO: parameterize this a bit more rather than pasting in the values in the def line
# TODO: retool this as either load_register_by_id / _url or add it as a param
def load_register(doc_id='1vrvYjcNLTdWypfeWjMUyMuo3LVtX80CKMRY-3SegdDw', tab_name='History Register', id_col_num=5,
                  cred_file='./service-account.json', delay=0):
    """
    Opens a History Register google document, with a specified Google Sheet ID, and loads the doc IDs from the specified
    column. By default this will load the central OffCom History Register.
    :param doc_id: Google Sheet ID of the History Register
    :param tab_name: Google Sheet ID of the History Register
    :param id_col_num: The column in the Register that contains the Google Sheet IDs of the individual history docs
    :param cred_file: The JSON file with the service account credentials in it
    :param delay: The number of seconds to pause between each official - this exists only to stick within the Google API quota
    :return: A list of loaded history docs
    """
    start = datetime.datetime.now()

    # authenticate with google and open their geocoding service
    gc = util.authenticate_with_google(cred_file)
    google_api = util.connect_to_geocode_api()

    # Open the History Register
    register = gc.open_by_key(doc_id)
    register_tab = register.worksheet(tab_name)

    # Collect the list of Google Sheet IDs for the officials in the Register
    doc_ids = register_tab.col_values(id_col_num)
    doc_ids = filter(None, doc_ids[1:])  # remove header and blank entries from the list
    first = datetime.datetime.now()
    print u'Getting list of Register IDs took {}'.format(first - start)
    step = datetime.datetime.now()

    # Go through the list of Google Sheet IDs and load each one in turn
    officials = list()
    for doc in doc_ids:
        time.sleep(delay)  # sleep delay even before the first OHD because even failures count, plus the Register
        try:
            history = gc.open_by_key(doc)
            ver = util.get_version(history)
            # currently this only servers v3 sheets, so skip over any that aren't v3
            if ver != 3:
                print u'Found a doc with the wrong version: {}'.format(history.title)
                continue

            profile_values = history.worksheet('Profile').get_all_values()
            [pref_name, derby_name, legal_name] = util.get_names(profile_values)
            off = official.Official(history.id)
            off.pref_name = unicode(pref_name)
            off.derby_name = unicode(derby_name)
            off.legal_name = unicode(legal_name)
            profile_tab = history.worksheet('Profile')
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
            # add all the games from the Game History tab
            off.add_history(history.worksheet('Game History'))
            # add this official to the list
            officials.append(off)

            # profiling and testing/verification section
            print u'Loading {} took {}'.format(off.pref_name, datetime.datetime.now() - step)
            step = datetime.datetime.now()
        except gspread.exceptions.GSpreadException as e:
            print u"Can't open {} because: {}".format(doc, e.message)
        except Exception as e:
            print u"Can't open {} because of generic error: {}".format(doc, e.message)

    # return the loaded officials
    return officials


if __name__ == '__main__':
    start = datetime.datetime.now()
    # get the location info
    # locations = util.load_locations(cred_file='../service-account.json')

    # doc_id = '1Ev7YChLm8MHC6mKkSy6gFsVcGuzOneL4DzY3H_OFZCQ'  # test register
    doc_id = '1mH2Sui25qqrGt0x8k_KuL1BONsZ3rOPCIurECnX3Jxw'  # real test register
    tab_name = 'Test History Register'
    # tab_name = 'History Register'
    delay = 2.5

    o = load_register(doc_id=doc_id, cred_file='../service-account.json', tab_name=tab_name, delay=delay)
    print u'Officials loaded, there are {} in the list, for a total of {} games'.format(len(o), reduce(lambda x, y: x+y, [len(x.games) for x in o]))
    # print u'Officials loaded, there are {} in the list'.format(len(o))
    step = datetime.datetime.now()
    print u'Full loading took {}'.format(step - start)
    query_string = [{'standard': [True], 'assn': ['WFTDA', 'MRDA']},
                    {'type': ['Other'], 'role': ['THR', 'ATHR', 'THSNO', 'ATHNSO']},
                    {"start": start.date(), "interval": 12, "max_interval": 5}]
    for off in o:
        qgames = util.query_history(off.positions, *query_string)
        if len(qgames[0]) > 0:
            print u'{}\'s breakdown of {} is {}'.format(off.pref_name, qgames[0][0].__class__, map(len, qgames))
        else:
            print u'{} has an empty games history'.format(off.pref_name)
    print u'Processing took an extra {}'.format(datetime.datetime.now() - step)

