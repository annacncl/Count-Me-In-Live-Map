#!/usr/bin/env python3
"""
Convert Count-Me-In CSV form export to orgs.json for the map.
Usage: python3 convert_orgs.py input.csv output.json
"""

import csv, sys, json, us, addfips

af = addfips.AddFIPS()

SCOPE_MAP = {
    'Local / County / Tribal 1+': 'local',
    'Nationwide': 'national',
    'Statewide 1+': 'statewide',
}

# Maps form tribal nation names -> Census TIGER GeoJSON NAME field
TRIBAL_NAME_MAP = {
    # Wisconsin
    'Oneida Nation': 'Oneida (WI)',
    'Menominee Indian Tribe': 'Menominee',
    'Ho-Chunk Nation': 'Ho-Chunk Nation',
    'Bad River Band': 'Bad River',
    'Red Cliff Band': 'Red Cliff',
    'Lac du Flambeau': 'Lac du Flambeau',
    'St. Croix Chippewa': 'St. Croix',
    'Sokaogon Chippewa': 'Sokaogon Chippewa',
    'Stockbridge-Munsee': 'Stockbridge Munsee',
    'Forest County Potawatomi': 'Forest County Potawatomi',
    # Minnesota
    'Red Lake Nation': 'Red Lake',
    'Leech Lake Band': 'Leech Lake',
    'White Earth Nation': 'White Earth',
    'Mille Lacs Band': 'Mille Lacs',
    'Fond du Lac Band': 'Fond du Lac',
    'Grand Portage Band': 'Grand Portage',
    'Bois Forte Band': 'Bois Forte',
}

def normalize_tribal_name(form_name):
    return TRIBAL_NAME_MAP.get(form_name.strip(), form_name.strip())

def convert(input_csv, output_json):
    with open(input_csv, encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))

    orgs = []
    errors = []
    tribal_warnings = []

    for i, row in enumerate(rows):
        name = row.get('Organization Name', '').strip()
        if not name:
            continue

        scope_raw = row.get('Where does your organization primarily work?', '').strip()
        scope = SCOPE_MAP.get(scope_raw, 'local')

        fips_list = []
        for col, val in row.items():
            if ('Counties' in col or 'County' in col) and val.strip():
                state_name = col.replace(' Counties (select all that apply)', '').replace(' Counties', '').strip()
                state = us.states.lookup(state_name)
                if not state:
                    continue
                for county in [c.strip() for c in val.split(',') if c.strip()]:
                    fips_str = af.get_county_fips(county, state=state.abbr)
                    if fips_str:
                        fips_list.append(int(fips_str))
                    else:
                        errors.append(f"  Could not find FIPS: {county}, {state.abbr} (org: {name})")

        tribal_data = []
        for col, val in row.items():
            if 'Tribal' in col and val.strip():
                for nation in [n.strip() for n in val.split(',') if n.strip()]:
                    census_name = normalize_tribal_name(nation)
                    tribal_data.append(census_name)
                    if census_name not in TRIBAL_NAME_MAP.values():
                        tribal_warnings.append(f"  '{nation}' not in TRIBAL_NAME_MAP (org: {name})")

        states_list = []
        if scope == 'statewide':
            state_val = row.get('What state(s)? Local', '').strip()
            if state_val:
                states_list = [s.strip() for s in state_val.split(',') if s.strip()]

        org = {
            'id': int(row.get('Id', i)),
            'name': name,
            'website': row.get('Organization Website', '').strip(),
            'scope': scope,
            'city': row.get('City', '').strip(),
            'hqState': row.get('State', '').strip(),
            'mission': '',
            'fips': fips_list,
            'states': states_list,
        }
        if tribal_data:
            org['tribalNations'] = tribal_data

        orgs.append(org)

    with open(output_json, 'w') as f:
        json.dump(orgs, f, indent=2)

    print(f"Converted {len(orgs)} organizations -> {output_json}")
    if errors:
        print(f"FIPS issues: {errors}")
    if tribal_warnings:
        print(f"Tribal name warnings (add to TRIBAL_NAME_MAP): {tribal_warnings}")
    else:
        print("All tribal names resolved successfully")

if __name__ == '__main__':
    input_csv = sys.argv[1] if len(sys.argv) > 1 else 'input.csv'
    output_json = sys.argv[2] if len(sys.argv) > 2 else 'orgs.json'
    convert(input_csv, output_json)
