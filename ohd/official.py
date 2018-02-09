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
        a list of Games officiated
        a list of Positions officiated
        a list of Events officiated
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
        self.positions = []
        self.secondary_positions = []
        # TODO: the game counts become complex, so maybe don't pre-cache them and calculate them each time, with flags for game/position and role/fam?
        # self.game_count = 0
        # self.position_count = 0
        # self.ref_position_count = 0
        # self.nso_position_count = 0
        # self.weighting = {}
        # self.qualified_games = {}

    def __repr__(self):
        return u'<name: {}, refcert {}, nsocert: {}, games {}>'.format(self.pref_name, self.refcert, self.nsocert,
                                                                       len(self.games))

    def add_history(self, history_tab):
        """
        Takes the entire Game History tab and runs through it, adding each game to the Official
        :param history_tab: opened Game History tab
        :return: None
        """
        # Get all the rows apart from the top line (header)
        history = history_tab.get_all_values()[1:]
        # Sort list by date (descending)
        history.sort(reverse=True)
        # Iterate through the official's history, one at a time until there's no more
        for item in history:
            try:
                # Validation: Is it a date?
                gdate = datetime.datetime.strptime(item[0], '%Y-%m-%d').date()
                # print u'Date for the game is {}.'.format(datetime.datetime.strptime(game[0], '%Y-%m-%d').date())
                assn = item[6]
                gtype = item[7]
                role = item[8]
                role_secondary = item[9]
                software = item[10]
                self.add_game(Game(gdate, assn, gtype, role, role_secondary, software, item))
                # TODO: Calculate Positions as a separate class? Or is that just a query result?
                # TODO: Event based experience
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
        # Add the Game to the history
        self.games.append(game)

        # Add the primary Position to the history
        self.positions.append(game)
        # Add the primary Position to the history (if there was one)
        if game.role_secondary:
            self.secondary_positions.append(game)

    # TODO: how to handle age? Add it to the filter_map and pop it out before the for loop? Ask for start and end date? As for start date and period (year) and split the results as a list based on the number of periods there are?
    def get_games(self, filter_map={}):
        """
        Returns a list of Games, that meet the criteria in the provided filter_map.
        :param filter_map: A dict of property: list of values to select if matched
        :return: a list of Games
        """
        proto_list = self.games
        # iterate through each element in the filter_map, and filter on the values
        for item in filter_map.keys():
            proto_list = [i for i in proto_list if getattr(i, item) in filter_map[item]]
        return proto_list


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
        if assn not in assns:
            self.standard = False
        if gtype not in types:
            self.standard = False
        if role not in roles:
            self.standard = False

    def __repr__(self):
        if self.standard:
            return u'<Assn {}, Role {}>'.format(self.assn, self.role)
        else:
            return u'<Assn {}, Role {}, non-std>'.format(self.assn, self.role)
