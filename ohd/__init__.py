__author__ = 'hammer'
__version__ = '1.0a'

import config
import util

# from util import get_names
# from util import get_version
from util import query_history
from util import GoogleSheet

from official import Official, Game, Position, Event

from register import load_register
from register import load_official
from register import ohd_conn_generator

# TODO: refactor Central Officiating Informatics Library (COIL): coil.officials coil.leagues
