"""
CONFIG:
All the configuration items for the application, and the runtime config object.
"""
__author__ = 'hammer'

import json
import logging
import pygsheets
import sqlalchemy
import datetime
import pandas as pd
from pathlib import Path
# from . import util


class Conf:
    def __init__(self):
        """
        Initialize the config object and attach its runtime version to the module as a singleton
        """
        self.google = self.Google()
        self.runtime = self.Runtime()
        self.caching = self.Cache()
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

    ##########
    # SECTION: Cache
    class Cache:
        """
        Class to manage the runtime cache, and helper functions
        """
        file = None
        cache = None
        engine = None
        cache_only_mode = True  # Flag set to force cache values to be used

        # OHD Register sheet format (columns)
        reg_tab_list = ['Email Address', 'Derby Name', 'Legal Name', 'History URL', 'History ID', 'Created', 'Last Game',
                        'Last sync (seconds since epoch)', 'Template Version', 'Imported History URL', 'Picture URL']

        # OHD Game History tab format (columns)
        history_tab_list = ['Date', 'Event Name', 'Event Location', 'Event Host', 'Home / High Seed', 'Visitor / Low Seed',
                            'Association', 'Game Type', 'Position', '2nd Position', 'Software', 'Head Referee', 'Head NSO',
                            'Notes']

        # OHD Profile information format (columns)
        history_officials_data_list = ['ID', 'Name_Preferred_raw', 'Pronoun_raw', 'Name_Derby', 'Name_Legal', 'Location_raw',
                                       'Affiliated_League_raw', 'Cert_Ref_raw', 'Endorsements_Ref_raw', 'Cert_NSO_raw',
                                       'Endorsements_NSO_raw', 'Officiating_Number', 'Email_Address', 'Phone_Number',
                                       'Insurance_Derby', 'Association_Affiliations_raw']

        # cache / final data columns
        cache_defs = dict()
        # TODO: Index cols as well?
        cache_defs['metadata'] = {'cols': ['last_update'],
                                  'dates': ['last_update']}
        cache_defs['register'] = {'cols': reg_tab_list,
                                  'dates': []}
        cache_defs['officials'] = {'cols': history_officials_data_list,
                                   'dates': []}
        cache_defs['game_data'] = {'cols': ['off_id'] + history_tab_list,
                                   'index': ['off_id', 'Date'],
                                   'dates': ['Date']}

        def init_cache(self):
            """
            Initalizes a connection to the cache, and creates an empty cache if there is no cache.
            The environment will need to be initialized first.
            """
            try:
                file = Path(self.file)
            except TypeError:
                # self.logger.error("Environment hasn't been configured, run init_env()")
                raise Exception("Environment needs to be configured first")
            engine = sqlalchemy.create_engine(f"sqlite:///{file}")
            cache = dict()
            self.engine = engine

            # load each of the defined caches
            cache_defs = self.cache_defs
            for key in cache_defs:
                conf.logger.debug(f"initializing cache:{key}")
                if engine.has_table(key):
                    conf.logger.debug(f"Getting {key} from database")
                    cache[key] = pd.read_sql_table(key, engine, parse_dates=cache_defs[key]['dates'])
                else:
                    conf.logger.debug(f"Making {key} from scratch")
                    df = pd.DataFrame(columns=cache_defs[key]['cols'])
                    if 'index' in cache_defs[key]:
                        conf.logger.debug(f"making index of: {cache_defs[key]['index']}")
                        df = df.set_index(cache_defs[key]['index'])
                    cache[key] = df
            self.cache = cache
            # conf.logger.debug("Pre-persist")
            # self.persist_cache()
            # conf.logger.debug("Post-persist")

        def fetch(self, cache_key, item):
            """
            Queries the cache and returns an item from the cache, if it is found and if it is considered sufficicently current.
            :param cache_key: the partition of the cache to search
            :param item: the cache item key to fetch
            :return: the cached item, if found, and None if no current item found
            """
            if item in self.cache[cache_key].index:
                if conf.runtime.force_refresh:
                    # ignore cache, force the loading of data
                    return None
                if self.cache_only_mode:
                    # force the use of cached values
                    return self.cache[cache_key].loc[item]
                if cache_key == 'game_data':
                    # game data recency isn't tracked in the metadata, so bypass the stale check
                    return self.cache[cache_key].loc[item]
                cache_expiry = datetime.datetime.now() - datetime.timedelta(days=conf.runtime.stale_days)
                if self.cache['metadata'].loc[item]['last_update'] > cache_expiry:
                    # return the cached value only if it's not "stale"
                    return self.cache[cache_key].loc[item]
            return None

        def persist_cache(self):
            """
            Persists the in memory cache to disk.
            """
            # save each of the defined caches to disk
            cache_defs = self.cache_defs
            for key in cache_defs:
                if not self.cache[key].empty:
                    logging.debug(f"Persisting {key} to the cache")
                    self.cache[key].to_sql(key, self.engine, if_exists='replace')

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
    def init_env(self, env_name, data_dir="./data", with_keys=False, stale_days_override=None):
        """
        At runtime, initialize the Config object to match the runtime environment configuration.
        This should be the first thing when using the OHD module.
        :param env_name: The name of an environment to configure
        :param data_dir: directory where data files will be looked for; by default "./data"
        :param with_keys: if True, will import the API keys from the data_dir
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

        # Action flags
        if with_keys:
            self.import_keys()

        # Overrides
        if stale_days_override:
            self.runtime.stale_days = stale_days_override

        # Initialize the cache
        self.caching.file = data_path / f"ohd-cache-{env_name}.db"
        self.caching.init_cache()

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


# runtime config object
conf = Conf()
