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
        self.events = []
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
        Adds a Game to the official's history. Also triggers adding of Positions and Events relating to that Game
        :param game: Game object
        :return: None
        """
        # Add the Game to the history
        self.games.append(game)

        # Add the Positions for this Game to the history
        self.add_position(game)

        # Add the Events for this Game to the history
        # TODO: self.add_event(game)

    def add_position(self, game):
        """
        Adds Positions (primary and secondary) to the official's history.
        Triggered by add_game() and not expected to be run outside of that context.
        :param game: Game object
        :return:
        """
        # Add the primary Position to the history
        self.positions.append(Position(game.gdate, game.assn, game.type, game.role, 'p', game.software, game.raw_row))
        # Add the secondary Position to the history (if there was one)
        if game.role_secondary:
            self.positions.append(
                Position(game.gdate, game.assn, game.type, game.role_secondary, 's', game.software, game.raw_row))

    # TODO: how to handle age? Add it to the filter_map and pop it out before the for loop? Ask for start and end date? As for start date and period (year) and split the results as a list based on the number of periods there are?
    def query_history(self, scope, filter_map=None):
        """
        Returns a list of Games, that meet the criteria in the provided filter_map.
        :param scope: A text label, querying Games, Positions or Events
        :param filter_map: A dict of { property: list of values } to select if matched
        :return: a list of Games
        """
        if filter_map is None:
            filter_map = []

        try:
            proto_list = getattr(self, scope)
            # iterate through each element in the filter_map, and filter on the values
            for item in filter_map.keys():
                proto_list = [i for i in proto_list if getattr(i, item) in filter_map[item]]
            return proto_list
        except Exception as e:
            print u'History query failed. Scope: {}, Error: {}'.format(scope, e)


class Game:
    """
    Each official will have a history made up of many Games, which store the relevant information about each game.
    Each Game can have one or two Positions and may have one Event (recorded separately).
    """
    def __init__(self, gdate, assn, gtype, role, role_secondary, software, raw_row):
        # default "error" values
        self.gdate = None
        self.assn = None
        self.type = None
        self.role = None
        self.role_secondary = None
        self.software = None
        self.standard = False
        self.raw_row = list()

        # If the date is valid, then populate the data as supplied
        if not isinstance(gdate, datetime.date):
            raise Exception('Game Date is not a datetime :{}'.format(gdate))
        # Is the raw list is actually a list, as expected
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
            return u'<Game: Assn {}, Role {}>'.format(self.assn, self.role)
        else:
            return u'<Game: Assn {}, Role {}, non-std>'.format(self.assn, self.role)


class Position:
    """
    Each official will have a history made up of many Positions worked, each of which store the relevant information
    about the game. Each Position can be primary or secondary.
    """
    def __init__(self, gdate, assn, gtype, role, primacy, software, raw_row):
        # default "error" values
        self.gdate = None
        self.assn = None
        self.type = None
        self.role = None
        self.primacy = None
        self.software = None
        self.standard = False
        self.raw_row = list()

        # If the date is valid, then populate the data as supplied
        if not isinstance(gdate, datetime.date):
            raise Exception('Position Date is not a datetime :{}'.format(gdate))
        # Is the primacy is correct
        if primacy not in ['p', 's']:
            raise Exception('Position Primacy is not a datetime :{}'.format(primacy))
        # Is the raw list is actually a list, as expected
        if not isinstance(raw_row, list):
            raise Exception('Position data is not a list, instead it\'s a {}'.format(type(raw_row)))

        self.gdate = gdate
        self.assn = assn
        self.type = gtype
        self.role = role
        self.primacy = primacy
        self.software = software
        self.raw_row = raw_row
        self.standard = True

        # If any of the data is not "standard" then mark the game as non-standard
        if assn not in assns:
            self.standard = False
        if gtype not in types:
            self.standard = False
        if role not in roles:
            self.standard = False

    def __repr__(self):
        if self.standard:
            return u'<Position: Assn {}, Role {}>'.format(self.assn, self.role)
        else:
            return u'<Position: Assn {}, Role {}, non-std>'.format(self.assn, self.role)
