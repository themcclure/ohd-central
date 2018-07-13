"""
This loads the officials from the Register and runs some reports/stats on them
"""
__author__ = 'hammer'

import ohd
import datetime


if __name__ == '__main__':
    start = datetime.datetime.now()
    # get the location info
    # locations = util.load_locations(cred_file='../service-account.json')

    doc_id = '1vrvYjcNLTdWypfeWjMUyMuo3LVtX80CKMRY-3SegdDw'  # production register
    # doc_id = '1mH2Sui25qqrGt0x8k_KuL1BONsZ3rOPCIurECnX3Jxw'  # real test register
    # tab_name = 'Test History Register'
    tab_name = 'History Register'
    ohd.config.google_api_delay = 10.0
    ohd.config.cred_file = './service-account.json'

    olist = ohd.load_register(doc_id=doc_id, tab_name=tab_name)
    officials = list()
    for history in ohd.ohd_conn_generator(olist):
        officials.append(ohd.register.load_official(history))
        print u'Processing {} took {}'.format(len(officials), datetime.datetime.now() - start)

    inactive_list = list()
    for o in officials:
        if len(o.games) > 0:
            g = reduce(lambda a, b: a.gdate > b.gdate and a or b, o.games)
            if g.gdate < datetime.date(2018, 1, 1):
                inactive_list.append([o.pref_name,g.gdate])
        else:
            # add the history docs without any games to the list
            inactive_list.append([o.pref_name,0])
    print inactive_list
