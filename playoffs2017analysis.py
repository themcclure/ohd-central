"""
This script inputs a summary of all the position applicants for playoffs 2017
"""
__author__ = 'hammer'

import ohd
import datetime
import pickle
import matplotlib
import matplotlib.gridspec as gridspec
import seaborn as sns
import pandas as pd
from sklearn.cluster import KMeans
from fuzzywuzzy import process as fuzzymatch

# try:
#     from matplotlib import verbose
# except Exception as e:
#     from matplotlib import Verbose
#     matplotlib.verbose = Verbose()
import matplotlib.pyplot as plt
import numpy as np

master_doc_id = '1j1iWsIJGA6Oh48h1c36WGqcOuT5xKhygywFDRxO0tuo'

playoff_ids = [
    '1g-KUXUebuhOqeNDrBXbhaMyMvlOTOnFEH0IUMT8f15E',  # seattle
    '1TnlJ-lw3CvyPp69Qyzdij9hLzOpzxYrnsz6f2_1XVV8',  # champs
    '1BIASTtcgtSZTDRu7CQOi9jAaSYj0xHcAzt7-ESrwduM',  # dallas
    '1phZVyCXTvTsVwFWkfCqYZvXOcaY7SOs59rL80pRM8BA',  # malmo
    '16uP5JlZGeqcIr_vHwaHupexNdQ8AoFDiv0OOZ4kLVKI',  # pittsburg
]
use_pickled_roles = True
# use_pickled_roles = False

if __name__ == '__main__':
    start = datetime.datetime.now()
    step = datetime.datetime.now()
    role = dict()
    roles_refs = ['CHR', 'JR', 'OPR', 'IPR', 'ALTR']
    roles_nsos = ['CHNSO', 'PT', 'PW', 'JT', 'SO', 'SK', 'LT', 'PBM', 'PBT', 'PLT', 'ALTN']
    roles = roles_nsos + roles_refs
    for r in roles:
        role[r] = dict()

    if use_pickled_roles:
        with open('roles.pickle', 'rb') as handle:
            role = pickle.load(handle)
    else:
        gc = ohd.util.authenticate_with_google('./service-account.json')

        # open the master doc, as it's the only place where the game counts ae kept
        master_sheet = gc.open_by_key(master_doc_id)
        # build the list of names -> game counts
        master_list = dict()
        for tab in [x.title for x in master_sheet.worksheets()]:
            if tab not in roles:
                continue
            if tab not in master_list:
                master_list[tab] = dict()
            tabsheet = master_sheet.worksheet(tab)
            for line in tabsheet.get_all_values()[1:]:
                if not line[0]:
                    continue
                if not line[1]:
                    line[1] = 0
                master_list[tab][line[0]] = [int(line[1]), float(line[2]), int(line[3])]

        # iterate and open all the history docs
        for sid in playoff_ids:
            sheet = gc.open_by_key(sid)
            tab_names = [x.title for x in sheet.worksheets()]
            for r in roles:
                if r not in tab_names:
                    continue
                rolesheet = sheet.worksheet(r)
                for line in rolesheet.get_all_values()[1:]:
                    if not line[0]:
                        continue
                    # some jerks put in different names in their history doc to their application form. Jerks
                    if line[0] not in master_list[r].keys():
                        line[0], confidence = fuzzymatch.extractOne(line[0], master_list[r].keys())
                    role[r][line[0]] = master_list[r][line[0]]
                    if role[r][line[0]][0] > 5:
                        role[r][line[0]][0] = 0
            print u'Sheet took {}'.format(datetime.datetime.now() - step)
            step = datetime.datetime.now()
        # local cache of the docs
        with open('roles.pickle', 'wb') as handle:
            pickle.dump(role, handle, protocol=pickle.HIGHEST_PROTOCOL)

    # convert the role to pandas format:
    rdata = pd.DataFrame(columns=['name', 'role', 'cert', 'wgt', 'cnt', 'cluster', 'cluster_label'])
    for r in role:
        for n in role[r]:
            rdata = rdata.append(pd.DataFrame(data=[n, r, role[r][n][0], role[r][n][1], role[r][n][2], np.nan, np.nan],
                                 index=['name', 'role', 'cert', 'wgt', 'cnt', 'cluster', 'cluster_label']).T, ignore_index=True)

    # fix up the data types
    rdata['cert'] = rdata['cert'].astype(np.int)
    rdata['cnt'] = rdata['cnt'].astype(np.int)
    rdata['wgt'] = rdata['wgt'].astype(np.float64)
    # rdata['cluster'] = rdata['cluster'].astype(np.int)
    # TODO: This groupby averages all weights, even if it's a ref and nso, so it's not the best metric but good for now
    # wgt_means = rdata[['name','wgt']].groupby(['name']).mean()
    # rdata.join(wgt_means,'name',rsuffix='_mean')

    for r in roles:
        # notes: 6 clusters was determined experimentally as the natural number of peaks
        slice = rdata[rdata['role'].isin([r])]
        if not len(slice):
            continue
        km_cert = KMeans(4).fit(slice[['wgt','cert']])
        slice_cert = km_cert.labels_.astype(np.int)
        # km_cnt = KMeans(6).fit(slice[['wgt', 'cnt']])
        # slice_cnt = km_cnt.labels_.astype(np.int)
        # rdata.loc[rdata.role == r, 'cluster'] = slice_cert
        rdata.loc[rdata.role == r, 'cluster'] = slice_cert

    # take the clusters and label them from the heaviest weight to the lightest
    rdata = rdata.sort_values('wgt', ascending=False)
    for r in roles:
        if not len(rdata[rdata.role == r]):
            continue
        clusters_seen = dict()
        cl = 'A'
        clabel_list = list()
        for x in rdata[rdata.role == r].cluster:
            if x not in clusters_seen:
                clusters_seen[x] = cl
                cl = chr(ord(cl) + 1)
            clabel_list.append(clusters_seen[x])
        rdata.loc[rdata.role == r, 'cluster_label'] = clabel_list

    jfactor = 0.05
    sns.stripplot('wgt', 'role', hue='cluster_label', data=rdata[rdata.role.isin(roles_refs)], jitter=jfactor, alpha=.8)
    plt.title('Weighted Ref Experience: clustered around weighted experience and cert')
    plt.show()
    sns.stripplot('wgt', 'role', hue='cluster_label', data=rdata[rdata.role.isin(roles_nsos)], jitter=jfactor, alpha=.8)
    plt.title('Weighted NSO Experience: clustered around weighted experience and cert')
    plt.show()

    # g_refs = sns.factorplot(x='wgt', y='cnt', data=rdata[rdata['role'].isin(roles_refs)], kind='strip',
    #                         palette=sns.color_palette("cubehelix", 8), hue='cert', col='role', col_wrap=2)
    # plt.show()
    # g_nsos = sns.factorplot(x='wgt', y='cnt', data=rdata[rdata['role'].isin(roles_nsos)], kind='strip',
    #                         palette=sns.color_palette("cubehelix", 8), hue='cert', col='role', col_wrap=2)
    # plt.show()

    # correlative view of wgt value vs # eligible games
    # sns.jointplot(x='cnt', y='wgt', data=rdata, kind="kde")
    # sns.jointplot(x='cnt', y='wgt', data=rdata[rdata['role'].isin(roles_nsos)], kind="kde")
    # sns.jointplot(x='cnt', y='wgt', data=rdata[rdata['role'].isin(roles_refs)], kind="kde")

    # # plot a set of graphs for each role
    # fig = plt.figure(figsize=(4, 4*len(role)), dpi=150)
    # # heights = np.ones(len(role)*2)*2
    # # gs = gridspec.GridSpec(nrows=len(role)*2, ncols=2, height_ratios=heights)
    # gs = gridspec.GridSpec(nrows=len(role)*2, ncols=2)
    #
    # fig.suptitle('Distribution of Experience')
    # # plt.style.use('seaborn')
    #
    # # roles = ['OPR', 'IPR']
    # plot_row_num = 0
    # for r, vals in role.items():
    #     if not vals:
    #         continue
    #     srow = plot_row_num
    #     erow = plot_row_num + 1
    #     arr = np.array([v for k, v in vals.items()])  # numpy array
    #
    #     # histogram
    #     sp = fig.add_subplot(gs[srow:erow, 0:1])
    #     num_bins = 16
    #     # num_bins = 20
    #     # num_bins = 'auto'
    #     n, bins, patches = sp.hist(arr[:, 1], num_bins, alpha=0.8)
    #     sp.set_xlabel('Weighted Experience')
    #     sp.set_ylabel('Number of officials')
    #     sp.text(sp.get_xlim()[1]*0.8, sp.get_ylim()[1]*0.8, 'Role: {}\n# bins {}'.format(r, len(bins) - 1), fontsize=7)
    #     # plt.xlabel('Weighted Experience')
    #     # plt.ylabel('Number of officials')
    #
    #     # scatter
    #     sp = fig.add_subplot(gs[srow:erow, 1])
    #     # size_arr = (1+arr[:, 0])/10
    #     size_arr = map(int, (arr[:, 0] + 1) / 3)
    #     colour_arr = arr[:, 0]/5
    #     sp.scatter(arr[:, 2], arr[:, 1], c=colour_arr, alpha=0.5)
    #     # sp.hexbin(arr[:, 2], arr[:, 1], bins=100)
    #     # dd = pd.DataFrame(data=arr[:,1:],columns=['wgt','cnt'])
    #     # sns.jointplot(x='wgt',y='cnt',data=dd,kind='hex',gridsize=10)
    #     # sp.legend()
    #     sp.set_xlabel('Weighted Experience')
    #     sp.set_ylabel('# games')
    #     plot_row_num += 1
    # fig.show()
