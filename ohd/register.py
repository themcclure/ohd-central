"""Open the History Register, then load all the history docs listed in the Register."""

__author__ = 'hammer'

import gspread
import datetime
# OHD specific stuff
import util
import official


# TODO: add two new arguments, max_docs returned and starting_location, in case this ever gets too big
# TODO: parameterize this a bit more rather than pasting in the values in the def line
# TODO: retool this as either load_register_by_id / _url or add it as a param
def load_register(doc_id='1wsMCWX-HLvlsQwURaMU7Zszf6loM0DazDWA8NaZ2xxE', tab_name='History Register', id_col_num=5,
                  cred_file='./service-account.json'):
    """
    Opens a History Register google document, with a specified Google Sheet ID, and loads the doc IDs from the specified
    column. By default this will load the central OffCom History Register.
    :param doc_id: Google Sheet ID of the History Register
    :param tab_name: Google Sheet ID of the History Register
    :param id_col_num: The column in the Register that contains the Google Sheet IDs of the individual history docs
    :param cred_file: The JSON file with the service account credentials in it
    :return: A list of loaded history docs
    """

    start = datetime.datetime.now()
    gc = util.authenticate_with_google(cred_file)

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
        try:
            history = gc.open_by_key(doc)
            ver = util.get_version(history)
            # currently this only servers v3 sheets, so skip over any that aren't v3
            if ver != 3:
                print u'Found a doc with the wrong version: {}'.format(history.title)
                continue

            [pref_name, derby_name, legal_name] = util.get_names(history)
            off = official.Official(history.id)
            off.pref_name = unicode(pref_name)
            off.derby_name = unicode(derby_name)
            off.legal_name = unicode(legal_name)
            profile_tab = history.worksheet('Profile')
            off.officiating_number = profile_tab.acell('D4').value
            off.refcert = util.normalize_cert(profile_tab.acell('B8').value)
            off.nsocert = util.normalize_cert(profile_tab.acell('B10').value)
            # TODO: cert endorsements
            # off.refcert_endorsements = util.normalize_endorsements(profile_tab.acell('B9').value)
            # off.nsocert_endorsements = util.normalize_endorsements(profile_tab.acell('B11').value)
            # TODO: normalize pronouns
            off.pronouns = profile_tab.acell('B3').value
            # TODO: geo location magic goes here
            off.location = profile_tab.acell('B6').value
            off.league_affiliation = profile_tab.acell('B7').value
            off.add_history(history.worksheet('Game History'))

            officials.append(off)

            # profiling and testing/verification section
            print u'Loading {} took {}'.format(off.pref_name, datetime.datetime.now() - step)
            step = datetime.datetime.now()
        except gspread.exceptions.GSpreadException as e:
            print u"Can't open {} because {}".format(doc, e.message)
        except Exception as e:
            print u"Can't open {} because generic {}".format(doc, e.message)

    # return the loaded officials
    return officials


if __name__ == '__main__':
    start = datetime.datetime.now()
    o = load_register(cred_file='../service-account.json')
    print u'Officials loaded, there are {} in the list, for a total of {} games'.format(len(o), reduce(lambda x, y: x+y, [len(x.games) for x in o]))
    # print u'Officials loaded, there are {} in the list'.format(len(o))
    step = datetime.datetime.now()
    print u'Full loading took {}'.format(step - start)
    query_string = [{'standard': [True], 'assn': ['WFTDA', 'MRDA']},
                    {'type': ['Other'], 'role': ['THR', 'ATHR', 'THSNO', 'ATHNSO']},
                    {"start": start.date(), "interval": 12, "max_interval": 5}]
    for off in o:
        qgames = util.query_history(off.positions, *query_string)
        print u'{}\'s breakdown of {} is {}'.format(off.pref_name, qgames[0][0].__class__, map(len, qgames))
    print u'Processing took an extra {}'.format(datetime.datetime.now() - step)
