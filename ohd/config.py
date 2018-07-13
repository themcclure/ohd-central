"""
CONFIG:
List of known Associations, Game Types and Roles
"""
__author__ = 'hammer'

# TODO: If these values can be instead pulled like from the core history template, code and template would 100% be in sync
assns = ['WFTDA', 'MRDA', 'JRDA']
types = ['Champs', 'Playoff', 'Sanc', 'Reg', 'National', 'Other']
ref_roles = ['THR', 'ATHR', 'CHR', 'HR', 'IPR', 'JR', 'OPR', 'ALTR']
nso_family = dict()
nso_family['ch'] = ['CHNSO']
nso_family['pt'] = ['PT', 'PLT' 'PW', 'IWB', 'OWB']
nso_family['st'] = ['JT', 'SO', 'SK']
nso_family['pm'] = ['PBM', 'PBT', 'LT']
# nso_family_pt = ['PT', 'PW', 'IWB', 'OWB']
# nso_family_st = ['JT', 'SO', 'SK']
# nso_family_pm = ['PBM', 'PBT', 'LT']
nso_roles = ['THNSO', 'ATHNSO'] + nso_family['ch'] + nso_family['pt'] + nso_family['st'] + nso_family['pm'] + ['HNSO', 'ALTN']

roles = ref_roles + nso_roles

# Google Maps API Key
google_api_key = 'AIzaSyAFZuqxbBJ7GaPSBI3vAWRzi9yL9zFR9iQ'
cred_file = '../service-account.json'
google_api_delay = 2
locations = dict()
officials = list()
