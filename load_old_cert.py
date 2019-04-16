"""
This script loads in the OldCert officials information and produces a geojson (initially) representation of that info.
This is for pro-tem analysis of where the officials are located globally.

There is no demographic information available except for:
- Name
- League Affiliation
- OldCert level(s)
"""
__author__ = 'hammer'

import ohd
import datetime
from geojson import Point, Feature, FeatureCollection
from math import radians
# initially found, 921 refs, 911 NSOs
# now with added apprentice leagues: 952 refs, 953 NSOs

if __name__ == '__main__':
    start = datetime.datetime.now()

    # get the location info
    locations = ohd.util.load_locations(cred_file='./service-account.json')
    ohd.config.locations = locations
    print('Locations took {}'.format(datetime.datetime.now() - start))
    step = datetime.datetime.now()

    gc = ohd.util.authenticate_with_google('./service-account.json')
    old_cert_doc_id = '1Nv0UMugPqGEaDAwdz8dQtCIgzlR8gfM3i6Tp7ZSXzjo'
    old_cert_doc = gc.open_by_key(old_cert_doc_id)

    off_list = dict()
    offs_skipped = dict()
    offs_skipped['Independent'] = 0
    offs_skipped['Unknown League'] = 0
    offs_skipped['Duplicate'] = 0
    refs_sheet_names = ['OldCert Ref 1', 'OldCert Ref 2', 'OldCert Ref 3', 'OldCert Ref 4']
    nsos_sheet_names = ['OldCert NSO 1', 'OldCert NSO 2', 'OldCert NSO 3', 'OldCert NSO 4']
    for sheet_name in refs_sheet_names:
        refs_sheet = old_cert_doc.worksheet(sheet_name)
        for ref in refs_sheet.get_all_values()[1:]:
            if not ref[1] or ref[1].strip() == 'Independent':
                offs_skipped['Independent'] += 1
                continue
            loc = ohd.util.match_league(ref[1], locations)
            if loc not in locations:
                offs_skipped['Unknown League'] += 1
                continue
            offkey = ref[0] + "@" + loc
            if offkey not in off_list.keys():
                off_list[offkey] = dict()
                off_list[offkey]["description"] = ref[0]
                off_list[offkey]["league"] = loc
                off_list[offkey]["latitude"] = locations[loc][7]
                off_list[offkey]["longitude"] = locations[loc][8]
                off_list[offkey]["association"] = "WFTDA"
                off_list[offkey]["isref"] = True
                off_list[offkey]["isnso"] = False
                off_list[offkey]["nsocert"] = 0
                if ref[2] and "Level" in ref[2]:
                    off_list[offkey]["refcert"] = int(ref[2][6:7])
                    off_list[offkey]["maxcert"] = int(ref[2][6:7])
                else:
                    off_list[offkey]["refcert"] = 0
                    off_list[offkey]["maxcert"] = 0
            else:
                off_list[offkey]["isref"] = True
                if ref[2] and "Level" in ref[2]:
                    off_list[offkey]["refcert"] = int(ref[2][6:7])
                    if off_list[offkey]["refcert"] > off_list[offkey]["maxcert"]:
                        off_list[offkey]["maxcert"] = off_list[offkey]["refcert"]
    num_refs = len(off_list)
    print("Found {} Refs".format(num_refs))
    # for l in off_list.keys()[10:20]:
    #     print off_list[l]

    for sheet_name in nsos_sheet_names:
        nsos_sheet = old_cert_doc.worksheet(sheet_name)
        for nso in nsos_sheet.get_all_values()[1:]:
            if not nso[1] or nso[1].strip() == 'Independent':
                offs_skipped['Independent'] += 1
                continue
            loc = ohd.util.match_league(nso[1], locations)
            if loc not in locations:
                offs_skipped['Unknown League'] += 1
                continue
            offkey = nso[0] + "@" + loc
            if offkey not in off_list.keys():
                off_list[offkey] = dict()
                off_list[offkey]["description"] = nso[0]
                off_list[offkey]["league"] = loc
                off_list[offkey]["latitude"] = locations[loc][7]
                off_list[offkey]["longitude"] = locations[loc][8]
                off_list[offkey]["association"] = "WFTDA"
                off_list[offkey]["isref"] = False
                off_list[offkey]["isnso"] = True
                off_list[offkey]["refcert"] = 0
                if nso[2] and "Level" in nso[2]:
                    off_list[offkey]["nsocert"] = int(nso[2][6:7])
                    off_list[offkey]["maxcert"] = int(nso[2][6:7])
                else:
                    off_list[offkey]["nsocert"] = 0
                    off_list[offkey]["maxcert"] = 0
            else:
                off_list[offkey]["isnso"] = True
                if nso[2] and "Level" in nso[2]:
                    off_list[offkey]["nsocert"] = int(nso[2][6:7])
                    if off_list[offkey]["nsocert"] > off_list[offkey]["maxcert"]:
                        off_list[offkey]["maxcert"] = off_list[offkey]["nsocert"]

    print('Found {} NSOs'.format(len(off_list) - num_refs))
    print('OldCert alone took {} and skipped {} Independents and {} with no known league'.format(datetime.datetime.now() - step,
                                              offs_skipped['Independent League'], offs_skipped['Unknown League']))
    step = datetime.datetime.now()

    # now to spit it out as a geoJSON file
    off_features = list()
    for off in off_list:
        props = dict()
        props['description'] = off_list[off]['description']
        props['league'] = off_list[off]['league']
        props['association'] = off_list[off]['association']
        props['isref'] = off_list[off]['isref']
        props['isnso'] = off_list[off]['isnso']
        props['refcert'] = off_list[off]['refcert']
        props['nsocert'] = off_list[off]['nsocert']
        props['maxcert'] = off_list[off]['maxcert']
        try:
            longlat = (float(off_list[off]['longitude']), float(off_list[off]['latitude']))
        except Exception as e:
            print('Skipping {}, it has no lat/long. Actual error was:: {}'.format(off_list[off]['description'], e))
            continue
        f = Feature(geometry=Point(longlat), properties=props)  # GeoJSON wants long/lat in that order
        off_features.append(f)
    off_collection = FeatureCollection(off_features)
    # print off_collection
    print('GeoJSON took {}'.format(datetime.datetime.now() - step))
    step = datetime.datetime.now()

    # now get an array of radians for spatial analysis
    radian_arr = list()
    for f in off_features:
        radian_arr.append(map(radians, f['geometry']['coordinates']))
    # print radian_arr
    print('Radian conversion took {}'.format(datetime.datetime.now() - step))
    step = datetime.datetime.now()

    # map out the population of officials (quick & dirty edition)
    # building a dict of league = [num officials, num_uncert, num_lowcert, num_highcert (3+)]
    # TODO: instead use reduce?
    offpop = dict()
    get_officials_at_location = lambda l: offpop[l] if l in offpop else [0, 0, 0, 0]
    for off in off_list:
        league = off_list[off]['league']
        spread = get_officials_at_location(league)
        spread[0] += 1
        maxcert = off_list[off]['maxcert']
        if maxcert >= 3:
            spread[3] += 1
        elif maxcert > 0:
            spread[2] += 1
        else:
            spread[1] += 1
        offpop[league] = spread
    # go through the full list of leagues and add in their offpop
    leaguepop = dict()
    for l in locations:
        spread = get_officials_at_location(l)
        leaguepop[l] = [
            locations[l][3],  # Country
            l.encode('utf-8'),  # League name
            spread[0],  # number of officials
            spread[1],  # number of uncertified officials
            spread[2],  # number of low certified officials
            spread[3],  # number of high certified officials
        ]
    # print leaguepop
    import csv
    with open('leaguepop.csv', 'wb') as csvfile:
        output = csv.writer(csvfile)
        for l in leaguepop:
            output.writerow(leaguepop[l])
    print('Location population of officials took {}'.format(datetime.datetime.now() - step))
    step = datetime.datetime.now()

