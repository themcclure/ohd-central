"""
Module for reading, storing and processing officials data.
"""
__author__ = 'hammer'

import datetime
# CONFIG:
# list of known Associations, Game Types and Roles
import config
assns = config.assns
types = config.types
roles = config.roles


class Official:
    """The basic official object.
    Contains data about that official, and the set of games they've worked.
    It contains:
        a list of Games officiated:
            note that secondary positions officiated count here
        the processed weighting in each role (and NSO family), including secondary positions
    """

    def __init__(self, doc_id):
        # basic stats/info about the official
        self.key = doc_id
        self.pref_name = ''
        self.derby_name = ''
        self.legal_name = ''
        self.refcert = 0
        self.nsocert = 0
        self.games = []
        # TODO: the game counts become complex, so maybe don't precache them and calculate them each time, with flags for game/position and role/fam?
        self.game_count = 0
        self.position_count = 0
        self.ref_position_count = 0
        self.nso_position_count = 0
        # self.weighting = {}
        # self.qualified_games = {}

    def __repr__(self):
        return u'<name: %r, refcert %d, nsocert: %d, games %d>'.format(self.pref_name, self.refcert, self.nsocert, self.game_count)

    def add_games(self, history_tab):
        """
        Takes the entire Game History tab and runs through it, adding each game to the Official
        :param history_tab: opened Game History tab
        :return: None
        """
        # Get all the rows apart from the top line (header)
        games = history_tab.get_all_values()[1:]
        # Sort list by date (descending)
        games.sort(reverse=True)
        # Iterate through games, one at a time until there's no more
        for game in games:
            # TODO: validate the input data and form a Game, then add it to the Official
            try:
                # Validation: Is it a date?
                gdate = datetime.datetime.strptime(game[0], '%Y-%m-%d').date()

                # print u'Date for the game is {}.'.format(datetime.datetime.strptime(game[0], '%Y-%m-%d').date())
                self.add_game(game)
            except Exception as e:
                # This game isn't valid, go to the next one
                print u'Game not valid because: {}'.format(e)

    def add_game(self, game):
        """
        Adds a Game to the count. If it's a primary position game, update Ref, NSO and total counts
        Note that secondary positions don't count for game totals
        :param game: game row
        :return: None
        """
        self.games.append(game)
        self.game_count += 1
        # if game.primacy == 1:
        #     self.position_count += 1
        #     if game.role in config.ref_roles:
        #         self.ref_tally += 1
        #     elif game.role in config.nso_roles:
        #         self.nso_tally += 1


class Game:
    """
    Each official will have a history made up of many games
    Note:
        Age is the the number of whole years since the reference date (freezeDate)
        Primacy is 1 for games worked in the primary position, 2 for secondary positions
    """
    def __init__(self, gdate, assn, gtype, role, primacy):
        # default "error" values
        self.assn = None
        self.type = None
        self.role = None
        self.gdate = None
        self.primacy = None

        # if all the inputs are valid, then populate the data
        if (assn in assns) and (gtype in types) and (role in roles) and (isinstance(gdate, datetime.date)) and (primacy in (1,2)):
            self.assn = assn
            self.type = gtype
            self.role = role
            # self.age = age
            self.gdate = gdate
            self.primacy = primacy
        else:
            raise Exception('Validation of inputs failed... What? Too lazy to check the inputs for validity??')

    def __repr__(self):
        return "<Assn %s, Role %s>" % (self.assn, self.role)
