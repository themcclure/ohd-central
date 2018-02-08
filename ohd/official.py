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
            try:
                # Validation: Is it a date?
                gdate = datetime.datetime.strptime(game[0], '%Y-%m-%d').date()
                # print u'Date for the game is {}.'.format(datetime.datetime.strptime(game[0], '%Y-%m-%d').date())
                assn = game[6]
                gtype = game[7]
                role = game[8]
                role_secondary = game[9]
                software = game[10]
                self.add_game(Game(gdate, assn, gtype, role, role_secondary, software, game))
                # TODO: Calculate Positions as a separate class? Or is that just a query result?
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
    """
    def __init__(self, gdate, assn, gtype, role, role_secondary, software, raw_row):
        # default "error" values
        self.assn = None
        self.type = None
        self.role = None
        self.gdate = None
        self.role_secondary = None
        self.software = None
        self.standard = False
        self.raw_row = list()

        # If the date is valid, then populate the data as supplied
        if not isinstance(gdate, datetime.date):
            raise Exception('Game Date is not a datetime :{}'.format(gdate))
        if not isinstance(raw_row, list):
            raise Exception('Game data is not a list, instead it\'s a {}'.format(type(raw_row)))

        self.gdate = gdate
        self.assn = assn
        self.type = gtype
        self.role = role
        if len(role_secondary) > 0 and (role_secondary in roles):
            self.role_secondary = role_secondary
        self.software = software
        self.raw_row = raw_row
        self.standard = True
        # self.age = age

        # If any of the data is not "standard" then mark the game as non-standard
        if not assn in assns:
            self.standard = False
        if not gtype in types:
            self.standard = False
        if not role in roles:
            self.standard = False

    def __repr__(self):
        if self.standard:
            return u'<Assn {}, Role {}>'.format(self.assn, self.role)
        else:
            return u'<Assn {}, Role {}, non-std>'.format(self.assn, self.role)
