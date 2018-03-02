"""A collection of utility functions relating to the Officiating History Document."""
__author__ = "hammer"


import gspread
from oauth2client.service_account import ServiceAccountCredentials
# import googlemaps
import re
import datetime
from dateutil.parser import parse
from dateutil import relativedelta
from defaultlist import defaultlist
# OHD specific imports
from ohd import config
from geopy.geocoders import GoogleV3
# from geopy.distance import vincenty as distance
from geojson import Point, Feature, FeatureCollection


def authenticate_with_google(cred_file):
    """
    Authenticate the service account with google and return a credentialed connection.
    :return: Authorized gspread connection
    """
    scope = ['https://spreadsheets.google.com/feeds']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(cred_file, scope)

    return gspread.authorize(credentials)


def connect_to_geocode_api(key=config.google_api_key):
    """
    Takes the Google API key and returns a geocode api connection
    :param key: Google API key
    :return: connection to the geocode service
    """
    return GoogleV3(api_key=key)


def get_names(sheet):
    """
    Takes the Profile tab and extracts the official's set of names.
    :param sheet: the opened Google sheet object
    :return: a list of names
    """
    tab = sheet.worksheet('Profile')
    pref_name = tab.acell('B2').value
    derby_name = tab.acell('B4').value
    legal_name = tab.acell('B5').value
    if not pref_name and not derby_name:
        pref_name = legal_name
        derby_name = legal_name
    elif not pref_name:
        pref_name = derby_name
    elif not derby_name:
        derby_name = legal_name
    return [pref_name, derby_name, legal_name]


def get_version(sheet):
    """
    Check the info tabs and make a determination about which version of the officiating history document is being used.
    Different versions keep information in different places
    :param sheet: the opened Google sheet object
    :return: integer with the version number, or None for unknown version
    """
    tabs = [x.title for x in sheet.worksheets()]

    if 'Learn More' in tabs:
        return 3
    elif 'WFTDA Summary' in tabs:
        return 1
    elif 'Summary' in tabs:
        if 'WFTDA Referee' in tabs or 'WFTDA NSO' in tabs:
            # this is an old history doc but it's been modified to change the WFTDA Summary tab name
            return None
        elif 'Instructions' not in tabs:
            # this is a new history doc it's been modified to delete the instructions tab (a no no)
            return None
        elif sheet.worksheet('Instructions').acell('A1').value == 'Loading...':
            # found one instance where the Instructions tab was showing "loading" - at the moment this will only happen on the new sheets
            return 2
        elif 'Last Revised 2015' in sheet.worksheet('Instructions').acell('A104').value:
            return 2
        elif 'Last Revised 2016' in sheet.worksheet('Instructions').acell('A104').value:
            return 2
        elif 'Last Revised 2017-01-05' in sheet.worksheet('Instructions').acell('A102').value:
            return 2
        else:
            return None
    else:
        return None


def normalize_cert(cert_string):
    """
    Takes the cert string from the history, which is a freeform field, and normalizes it to 1-5 or 0 for not certified
    :param cert_string: string taken directly from the history sheet
    :return: 0-5
    """
    if cert_string is None:
        return 0
    # if it's already a number, return an int (if it's < 1 or greater than 5, return None)
    elif isinstance(cert_string, float) or isinstance(cert_string, int):
        if (cert_string < 1) or (cert_string > 5):
            return 0
        else:
            return int(cert_string)
    # if it's a string with numbers in it, return the first one
    numbers = re.findall(r'\d+', cert_string)
    if numbers:
        return int(numbers[0])
    else:
        # there are no numbers in the cell, look for someone spelling out the numbers by finding the word:
        if 'ONE' in cert_string.upper():
            return 1
        elif 'TWO' in cert_string.upper():
            return 2
        elif 'THREE' in cert_string.upper():
            return 3
        elif 'FOUR' in cert_string.upper():
            return 4
        elif 'FIVE' in cert_string.upper():
            return 5
        else:
            # there are no valid numbers in the string
            return 0


def query_history(items, filter_in=None, filter_out=None, filter_date=None):
    """
    Returns a list of items from the supplied list of items (Games, Positions, Events), that meet the criteria in the
    provided filters. All items are matched against the include list first, then any items matching against the exclude
    list are removed.
    Finally, if there is a date filter provided, items will be returned in a list of lists, grouped by items that lie in
    the different intervals
    :param items: list of items (Games, Positions, Events) to filter
    :param filter_in: A dict of { property: list of values } to select if matched
    :param filter_out: A dict of { property: list of values } to remove if matched
    :param filter_date: A dict of date properties to filter or group by:
                        start: start date (datetime.date) - everything more recent that that will be filtered out.
                                If start is not specified, then it will default to today's date.
                        interval: the size (number of months) of each "bucket" from the start date to group results
                                The first interval (start date - interval) is the 0th interval. If there is no interval
                                specified (or it is not an int >= 0) then all items will be returned in a single list
                        max_interval: the max_intervals number of intervals to go back, if there is no max_interval specified then
                                (or it is not an int > 0) it will count all the way back.
                                If there is no interval specified, max_interval is ignored.
    :return: a list of objects of type from scope (Games, Positions, Events)
    """
    if filter_in is None:
        filter_in = dict()
    if filter_out is None:
        filter_out = dict()
    if filter_date is None:
        filter_date = dict()
    proto_list = list()
    fdate = datetime.date.today()
    i = None

    try:
        proto_list = items
        # iterate through each element in filter_in, and select the matching values
        for item in filter_in.keys():
            proto_list = [i for i in proto_list if getattr(i, item) in filter_in[item]]
        # iterate through each element in filter_out, and remove the matching values
        for item in filter_out.keys():
            proto_list = [i for i in proto_list if hasattr(i, item) and getattr(i, item) not in filter_out[item]]
    except Exception as e:
        print u'History query failed. Item Type: {}, Error: {}, Item was {}'.format(type(items[0]), e, i.raw_row)

    try:
        if 'start' in filter_date and isinstance(filter_date['start'], datetime.date):
            # input overrides the default start date
            fdate = filter_date['start']

        if 'interval' not in filter_date or not isinstance(filter_date['interval'], int) or filter_date['interval'] <= 0:
            # there's no interval set, so just return all items that are older than the start date
            proto_list = [i for i in proto_list if i.gdate <= fdate]
        else:
            bucket_list = defaultlist(list)
            # process each item in the list, and group it into interval buckets
            for item in proto_list:
                # skip over the items before the filter date (fdate)
                if item.gdate > fdate:
                    continue
                datediff = relativedelta.relativedelta(fdate, item.gdate)
                bucket = (datediff.years * 12 + datediff.months) / filter_date['interval']
                bucket_list[bucket].append(item)

            max_intervals = len(bucket_list)
            if 'max_interval' in filter_date and isinstance(filter_date['max_interval'], int) and filter_date['max_interval'] > 0:
                # there is an interval but there's no max_interval, so return as many intervals as it takes to bucket all the items
                max_intervals = filter_date['max_interval']
            proto_list = bucket_list[:max_intervals]
    except Exception as e:
        print u'History query failed on date calcs for Item Type: {}, Error: {}'.format(type(items[0]), e)
    return proto_list


def normalize_officials_location(geocoding_service, location, league_affiliation):
    """
    Take the self reported location, and return the lat/long of that. If it does not exist, fall back to their league's
    location. If there is no affiliation either, then return (0,0)
    :param geocoding_service: the object link to the geocoding service
    :param location: the location from their Profile tab
    :param league_affiliation: the league affiliation from their Profile tab
    :return: a tuple (lat,long)
    """
    working_location = ''
    if location:
        working_location = location
    elif league_affiliation:
        # TODO: This is overkill - assuming several most people don't have multiple affiliations, maybe fuzzy matching here?
        if config.locations:
            working_location = [l for l in league_affiliation.split(',') if l.strip() in config.locations.keys()][0]
    else:
        return 0, 0

    loc = geocoding_service.geocode(working_location)
    if loc:
        return loc.latitude, loc.longitude
    else:
        return 0, 0


# TODO: refactor into spatial module
def load_locations(cred_file='./service-account.json'):
    """
    Queries the WFTDA League membership sheet for locations, and returns a dict of league name and location information.
    The source Google sheet is also updated with the latest location data.
    The returned dict also includes the known aliases.
    :param cred_file: location of the credentials file
    :return: a dict of league name and location info
    """
    start = datetime.datetime.now()
    doc_id = '1Nv0UMugPqGEaDAwdz8dQtCIgzlR8gfM3i6Tp7ZSXzjo'
    gc = authenticate_with_google(cred_file)

    # Open the raw list from the WFTDA
    location_doc = gc.open_by_key(doc_id)
    raw_leagues_doc = location_doc.worksheet('Full Members of WFTDA')
    # raw_leagues_doc = location_doc.worksheet('Test Members of WFTDA')
    raw_leagues = dict()
    # TODO: add Apprentice members
    # TODO: add MRDA and JRDA leagues
    for league in raw_leagues_doc.get_all_values()[1:]:
        if not league[0]:
            continue  # skip over rows with empty league names
        raw_row = ':'.join(league)
        raw_leagues[league[0]] = [
                                    league[0],  # League Name
                                    league[6],  # City,
                                    league[7],  # State/Province,
                                    league[8],  # Country,
                                    "WFTDA",  # Association
                                    league[1],  # Membership Class (APP will be for Apprentice membership)
                                    league[4],  # Full membership join date
                                    None,  # Latitute
                                    None,  # Longitude
                                    None,  # Google Place ID
                                    raw_row  # raw row
                                ]

    # Load the processed leagues
    processed_leagues_doc = location_doc.worksheet('League Locations')
    # processed_leagues_header_row = processed_leagues_doc.get_all_values()[0]
    processed_leagues_init = processed_leagues_doc.get_all_values()[1:]
    # turn the values list into a dict, removing lines with no league name
    processed_leagues = {l[0]: l for l in processed_leagues_init if l[0]}

    # Go through the list of leagues from the WFTDA and geocode only the missing or changed leagues
    google_api = connect_to_geocode_api()
    num_geocoded = 0
    for league in raw_leagues:
        # if the league is in the processed leagues, and if the raw rows (signature) match, and it has both lat and long, then skip it
        if league in processed_leagues and raw_leagues[league][-1] == processed_leagues[league][-1] \
                        and processed_leagues[league][7] and processed_leagues[league][8]:
            continue
        try:
            print u'Geocoding {}'.format(raw_leagues[league][0])
            raw_city = raw_leagues[league][1]
            # At least one league entered their Shire rather than the city, that's preventing a City match
            if raw_city[-5:] == 'shire':
                raw_city = raw_city[:-5]
            # geocode using City State/Province and Country
            loc = google_api.geocode(u', '.join([raw_city, raw_leagues[league][2], raw_leagues[league][3]]))
            place_id, city, state, country, latitude, longitude = normalize_location_from_geocode(loc)
            # sometimes the listed state is preventing preventing a location match, so if there's no match first, try without the state
            if not city:
                loc = google_api.geocode(u', '.join([raw_city, raw_leagues[league][3]]))
                place_id, city, state, country, latitude, longitude = normalize_location_from_geocode(loc)
            num_geocoded += 1
        except Exception as e:
            print u'EXCEPTION: Google geocode error {}'.format(e)
            print u'raw_league: {}'.format(raw_leagues[league])
            continue
        if loc:
            raw_leagues[league][1] = city
            raw_leagues[league][2] = state
            raw_leagues[league][3] = country
            start_date = raw_leagues[league][6]
            if start_date.lower().strip() == 'original':
                raw_leagues[league][6] = '2004-01-01'
            else:
                # parse out the first date looking string that can be found, or a four digit year (whichever is first)
                try:
                   start_date = re.findall('(\d+[/|-]\d+[/|-]\d+|\d{4})', start_date)[0]
                   raw_leagues[league][6] = parse(start_date).date().isoformat()
                except:
                    print u'Date format not recognized for league {}'.format(league)
            raw_leagues[league][7] = latitude
            raw_leagues[league][8] = longitude
            raw_leagues[league][9] = place_id
        processed_leagues[league] = raw_leagues[league]

    print u'Processed Leagues, geocoded {}, took {}'.format(num_geocoded, datetime.datetime.now() - start)
    num_leagues = len(processed_leagues) + 1
    num_cols = processed_leagues_doc.col_count
    processed_leagues_doc.resize(rows=num_leagues)
    source_range = processed_leagues_doc.range(2, 1, num_leagues, processed_leagues_doc.col_count)  # from the first data row, to the last
    target_range = list()

    # Go through each processed league, grab a row from the source range, edit the values and add it to the target range
    for league in processed_leagues:
        row = source_range[:num_cols]
        for i in range(len(processed_leagues[league])):
            row[i].value = processed_leagues[league][i]
        target_range.extend(row)
        del source_range[:num_cols]

    # Add the updated Cells to the Google sheet
    processed_leagues_doc.update_cells(target_range)

    # TODO: add the aliases to the processed list - or maybe just use fuzzy matching for leagues not found in the list? Both?
    # raw_aliases_doc = location_doc.worksheet('League Aliases')

    print u'Processing leagues and locations took {}'.format(datetime.datetime.now() - start)
    return processed_leagues


def build_league_geojson(leagues, filename=None):
    """
    Takes in a dict of leagues, and generates a GeoJSON file (if provided), and returns the GeoJSON object
    :param leagues: dict of leagues
    :param filename: filename, if provided, that the GeoJSON will be written to
    :return: GeoJSON object of the leagues provided
    """
    league_features = list()
    for league in leagues:
        l = leagues[league]
        loc_props = dict()
        loc_props["description"] = l[0]
        loc_props["city"] = l[1]
        loc_props["state/province"] = l[2]
        loc_props["country"] = l[3]
        loc_props["association"] = l[4]
        loc_props["class"] = l[5]
        loc_props["joined"] = l[6]
        try:
            longlat = (float(l[8]), float(l[7]))
        except Exception as e:
            print u'Skipping {}, it has no lat/long. Actual error was:: {}'.format(l[0], e)
            continue
        f = Feature(geometry=Point(longlat), properties=loc_props)  # GeoJSON wants long/lat in that order
        league_features.append(f)
    league_collection = FeatureCollection(league_features)

    if filename:
        # TODO: Add in file handling/saving
        pass

    return league_collection


def normalize_location_from_geocode(location, long_names=True):
    """
    Takes the JSON returned from geocoding and parses out the normalized Google Place ID, City, State/Province, Country,
    Latitude and Longitude values and returns them as a list.
    :param location: JSON object from geocoding
    :param long_names: If this is True, return the long_name versions, otherwise return the short_name versions
    :return: list of Google Place ID, City, State/Province, Country, Latitude, Longitude
    """
    if not location:
        return [None, None, None, None, None, None]
    name_type = 'short_name'
    if long_names:
        name_type = 'long_name'
    city = ''
    state = ''
    country = ''
    latitude = location.latitude
    longitude = location.longitude

    geo = location.raw
    place_id = geo['place_id']
    for component in geo['address_components']:
        if 'locality' in component['types']:
            city = component[name_type]
        elif 'administrative_area_level_1' in component['types']:
            state = component[name_type]
        elif 'country' in component['types']:
            country = component[name_type]
    return [place_id, city, state, country, latitude, longitude]
