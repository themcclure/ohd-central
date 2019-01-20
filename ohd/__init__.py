"""
The Officiating History Doc (OHD) module.

Used for working with and doing analysis on the available officiating data.

Usage best practices:
import ohd
from ohd.config import conf

conf.init_env("EnvName", "Data_Dir")

And if you have access to API and service-account keys:
conf.import_keys()
"""
__author__ = 'hammer'
__version__ = '2.0.0-alpha'

from . import config
# from . import util

# from util import get_names
# from util import get_version
from .util import authenticate_with_google
from .register import load_register
# from .register import load_histories
from .official import load_history

# TODO: refactor Central Officiating Informatics Library (COIL): coil.officials coil.leagues
