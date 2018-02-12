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
        self.events = dict()
        # TODO: the game counts become complex, so maybe don't pre-cache them and calculate them each time, with flags for game/position and role/fam?
        # self.game_count = 0
        # self.position_count = 0
        # self.ref_position_count = 0
        # self.nso_position_count = 0
        # self.weighting = {}
        # self.qualified_games = {}

    def __repr__(self):
        return '<name: {}, refcert {}, nsocert: {}, games {}>'.format(self.pref_name.encode('utf-8', 'ignore'),
                                                                       self.refcert, self.nsocert, len(self.games))

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
                # Validation: Is it a valid date?
                gdate = datetime.datetime.strptime(item[0], '%Y-%m-%d').date()
                # print u'Date for the game is {}.'.format(datetime.datetime.strptime(game[0], '%Y-%m-%d').date())
                self.add_game(Game(gdate, item))
            except Exception as e:
                # This game isn't valid, go to the next one
                print u'Game not valid because: {}'.format(e)

    def add_game(self, game):
        """
        Adds a Game to the official's history. Also triggers adding the Positions and Events relating to that Game
        :param game: Game object
        :return: None
        """
        # Add the Game to the history
        self.games.append(game)

        # Add the Positions for this Game to the history
        self.add_position(game)

        # Add the Events for this Game to the history
        self.add_event(game)

    def add_position(self, game):
        """
        Adds Positions (primary and secondary) to the official's history.
        Triggered by add_game() and not expected to be run outside of that context.
        :param game: Game object
        """
        # Add the primary Position to the history
        self.positions.append(Position(game, 'p'))
        # Add the secondary Position to the history (if there was one)
        if game.role_secondary:
            self.positions.append(Position(game, 's'))

    def add_event(self, game):
        """
        Adds an Event to the officials' history. Also, if the event already exists, increments the number of games counter.
        Triggered by add_game() and not expected to be run outside of that context.
        :param game: Game object
        """
        if not game.event_name:
            return
        if game.event_name not in self.events.keys():
            self.events[game.event_name] = Event(game.event_name, game)
        else:
            self.events[game.event_name].num_games += 1

    # TODO: how to handle age? Add it to the filter_map and pop it out before the for loop? Ask for start and end date? As for start date and period (year) and split the results as a list based on the number of periods there are?
    def query_history(self, scope, filter_map=None):
        """
        Returns a list of Games, that meet the criteria in the provided filter_map.
        :param scope: A text label, querying Games, Positions or Events
        :param filter_map: A dict of { property: list of values } to select if matched
        :return: a list of Games
        """
        if filter_map is None:
            filter_map = dict()

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
    # TODO: figure out how to differentiate game vs event based roles... eg a TH isn't a Game per-se but it IS an event based Position...
    def __init__(self, gdate, raw_row):
        # default "error" values
        self.standard = True

        # If the date is valid, then populate the data as supplied
        if not isinstance(gdate, datetime.date):
            raise Exception('Game Date is not a datetime: {}'.format(gdate))
        self.gdate = gdate
        # Is the raw list is actually a list, as expected
        if not isinstance(raw_row, list):
            raise Exception('Game data is not a list, instead it\'s a {}'.format(type(raw_row)))
        self.raw_row = raw_row

        # validate the vital stuff
        assn = unicode(raw_row[6].strip())
        gtype = unicode(raw_row[7].strip())
        role = unicode(raw_row[8].strip())
        if assn and gtype and role:
            self.assn = assn
            self.type = gtype
            self.role = role
        else:
            raise Exception('Association, Type and/or Role is empty: {}'.format([raw_row[0]]+raw_row[6:9]))

        # If any of the vital data is not "standard" then mark the game as non-standard
        if assn not in assns:
            self.standard = False
        if gtype not in types:
            self.standard = False
        if role not in roles:
            self.standard = False

        # non-vital stuff, just record what they write
        self.role_secondary = unicode(raw_row[9].strip())
        if self.role_secondary  and self.role_secondary not in roles:
            self.standard = False

        self.software = unicode(raw_row[10].strip())
        # TODO: add valid software options to config and check them here

        self.event_name = raw_row[1]
        self.event_location = unicode(raw_row[2].strip())
        self.event_host = (raw_row[3].strip())

    def __repr__(self):
        if self.standard:
            return '<Game: Assn {}, Role {}>'.format(self.assn, self.role)
        else:
            return '<Game: Assn {}, Role {}, non-std>'.format(self.assn, self.role)


class Position:
    """
    Each official will have a history made up of many Positions worked, each of which store the relevant information
    about the game. Each Position can be primary or secondary.
    """
    def __init__(self, game, primacy):
        self.gdate = game.gdate
        self.assn = game.assn
        self.type = game.type
        self.role = game.role
        self.software = game.software
        self.standard = game.standard
        self.primacy = primacy
        self.raw_row = game.raw_row

    def __repr__(self):
        if self.standard:
            return '<Position: Assn {}, Role {}>'.format(self.assn, self.role)
        else:
            return '<Position: Assn {}, Role {}, non-std>'.format(self.assn, self.role)


class Event:
    """
    Each official will have a history made up of many Events worked, each of which store the relevant information
    about the game
    """
    def __init__(self, name, game):
        self.name = name
        self.num_games = 1
        self.gdate = game.gdate  # TODO: or do we perhaps only really care about the year the event ran in?
        self.assn = game.assn
        self.type = game.type
        # self.role = game.role
        # self.standard = game.standard
        self.raw_row = game.raw_row

    def __repr__(self):
        return '<Event: {} ({})>'.format(self.name.encode('utf-8', 'ignore'), self.gdate.year)
