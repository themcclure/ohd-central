"""
CONFIG:
All the configuration items for the application, and the runtime config object.
"""
__author__ = 'hammer'

import json
import logging
import pygsheets
import sqlalchemy
import pandas as pd
from pathlib import Path
# from . import util


class Conf:
    def __init__(self):
        """
        Initialize the config object and attach its runtime version to the module (maybe better to use static class instead?)
        """
        self.google = self.Google()
        self.runtime = self.Runtime()
        self.logging = self.Logging()
        self.logger = self.logging.logger

    def __repr__(self):
        return f"Conf object, env: {self.runtime.label}"

    ##########
    # SECTION: Google API
    class Google:
        # Maps API Key
        maps_api_key = None

        # Service Account credentials
        cred_file = None

        # Authenticated connection
        client = None

        # runtime performance data
        runtime_api = list()  # for collecting the average time to process a google doc from API
        runtime_cache = list()  # for collecting the average time to process a google doc from the cache

        def get_client(self):
            """
            Returns the active connection to the Google Docs API, refreshing it if needed
            :return: an active
            """
            if not self.client:
                self.client = pygsheets.authorize(service_file=self.cred_file)
            return self.client

    ##########
    # SECTION: Runtime config
    class Runtime:
        data_dir = None
        locations = None
        officials = None
        reg_id = None
        reg_id_prod = '1vrvYjcNLTdWypfeWjMUyMuo3LVtX80CKMRY-3SegdDw'
        reg_id_test = '1mH2Sui25qqrGt0x8k_KuL1BONsZ3rOPCIurECnX3Jxw'
        reg_tab_name = None
        reg_tab_name_prod = 'History Register'
        reg_tab_name_test = 'Test History Register'  # This is the test tab name, in either test or prod docs, so it's likely to only be set explicitly

        stale_days = 10  # the number of days old cached data is before it's considered stale
        force_refresh = False  # if True, then fetch data live, regardless of what's in the cache

        label = 'Production'

        cache_file = None
        cache = None
        cache_only_mode = True  # Flag set to force cache values to be used

        # OHD Register sheet format (columns)
        reg_tab_list = ['Email Address', 'Derby Name', 'Legal Name', 'History URL', 'History ID', 'Created', 'Last Game',
                        'Last sync (seconds since epoch)', 'Template Version', 'Imported History URL', 'Picture URL']

        # OHD Game History tab format (columns)
        history_tab_list = ['Date', 'Event Name', 'Event Location', 'Event Host', 'Home / High Seed', 'Visitor / Low Seed',
                            'Association', 'Game Type', 'Position', '2nd Position', 'Software', 'Head Referee', 'Head NSO'
                            'Notes']

        # OHD non-game information format (columns)
        history_officials_data_list = ['Name']

        # cache / final data columns
        cache_defs = dict()
        # TODO: Index cols as well?
        cache_defs['metadata'] = {'cols': ['last_update'],
                                  'dates': ['last_update']}
        cache_defs['register'] = {'cols': history_officials_data_list + ['last_update'],
                                  'dates': ['last_update']}
        cache_defs['game_data'] = {'cols': history_tab_list,
                                   'dates': ['last_update']}

    ##########
    # SECTION: Logging
    class Logging:
        # set up logging
        log_format = '%(asctime)s:%(levelname)s:%(funcName)s:%(lineno)d: %(message)s'
        logging.basicConfig(format=log_format, datefmt='%m/%d/%Y %H:%M:%S')
        logger = logging.getLogger(__name__)
        logger.setLevel('INFO')

    ##########
    # SECTION: Helper functions
    def init_env(self, env_name, data_dir, stale_days_override=None):
        """
        At runtime, initialize the Config object to match the runtime environment configuration.
        This should be the first thing when using the OHD module.
        :param env_name: The name of an environment to configure
        :param data_dir: directory where data files will be looked for by default
        :param stale_days_override: An override for the stale_days parameter
        """
        self.logger.debug(f"Configuring runtime as: {env_name}")
        # runtime env specific config goes here
        data_path = Path(data_dir)

        # final env configuration goes here
        self.runtime.data_dir = data_path

        # configure items for the specified runtime environment
        if env_name == 'Test':
            self.logger.setLevel('DEBUG')
            self.logger.debug(f"Setting the runtime environment to {env_name} (full test)")
            self.runtime.label = 'Test'
            self.runtime.reg_id = self.runtime.reg_id_test
            self.runtime.reg_tab_name = self.runtime.reg_tab_name_prod
        elif env_name == 'TestTest':
            self.logger.setLevel('DEBUG')
            self.logger.debug(f"Setting the runtime environment to {env_name} (test test)")
            self.runtime.label = 'TestTest'
            self.runtime.reg_id = self.runtime.reg_id_test
            self.runtime.reg_tab_name = self.runtime.reg_tab_name_prod
        elif env_name == 'Prod':
            self.logger.debug(f"Setting the runtime environment to {env_name} (full prod)")
            self.runtime.label = 'Prod'
            self.runtime.reg_id = self.runtime.reg_id_prod
            self.runtime.reg_tab_name = self.runtime.reg_tab_name_prod
        elif env_name == 'ProdTest':
            self.logger.setLevel('DEBUG')
            self.logger.debug(f"Setting the runtime environment to {env_name} (test in prod)")
            self.runtime.label = 'ProdTest'
            self.runtime.reg_id = self.runtime.reg_id_prod
            self.runtime.reg_tab_name = self.runtime.reg_tab_name_test
        else:
            self.logger.error(f"Tried setting the runtime environment to {env_name} but that environment configuration was not found.")
            raise Exception(f"Configuration for the {env_name} environment cannot be found.")

        # Overrides
        if stale_days_override:
            self.runtime.stale_days = stale_days_override

        # Initialize the cache
        self.runtime.cache_file = data_path / f"ohd-cache-{env_name}.db"
        self.init_cache()

    def import_keys(self, api_keyfile=None, service_account=None):
        """
        Load API keys and Google credential files from the runtime environment
        :param api_keyfile: a JSON file with all the API keys needed stored in it
        :param service_account: a JSON Service Account keyfile
        """
        # load the API keys
        if not api_keyfile:
            api_keyfile = self.runtime.data_dir / 'keys.json'
        if api_keyfile.exists():
            api_keyfile = open(api_keyfile, 'r')
            api_keys = json.load(api_keyfile)
            self.google.maps_api_key = api_keys['google_map']

        # load the service account
        if service_account:
            service_account = Path(service_account)
        else:
            service_account = self.runtime.data_dir / 'service-account.json'
        self.runtime.cache_only_mode = False
        self.google.cred_file = service_account

    def init_cache(self):
        """
        Initalizes a connection to the cache, and creates an empty cache if there is no cache.
        The environment will need to be initialized first.
        :return: a connection to the cache
        """
        try:
            cache_file = Path(self.runtime.cache_file)
        except TypeError:
            self.logger.error("Environment hasn't been configured, run init_env()")
            raise Exception("Environment needs to be configured first")
        cache_engine = sqlalchemy.create_engine(f"sqlite:///{cache_file}")
        cache = dict()
        self.runtime.cache_engine = cache_engine

        # load each of the defined caches
        cache_defs = self.runtime.cache_defs
        for key in cache_defs:
            if cache_engine.has_table(key):
                cache[key] = pd.read_sql_table(key, cache_engine, parse_dates=cache_defs[key]['dates'])
            else:
                cache[key] = pd.DataFrame(columns=cache_defs[key]['cols'])
        self.runtime.cache = cache


# runtime config object
conf = Conf()
