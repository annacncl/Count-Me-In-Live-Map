#!/usr/bin/env python3
"""
fetch_orgs.py
-------------
Fetches org records from Airtable and converts them to orgs.json
for the Count-Me-In Live Map.

Usage:
    AIRTABLE_API_KEY=your_key python3 fetch_orgs.py
    
Or with explicit args:
    python3 fetch_orgs.py --output orgs.json
"""

import os, sys, json, csv, io, us, addfips, urllib.request, urllib.parse

af = addfips.AddFIPS()

AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
BASE_ID = 'app0PiJ1y0rvCPBXL'
TABLE_NAME = 'Count Me In Organizations Form'

SCOPE_MAP = {
    'Local / County / Tribal 1+': 'local',
    'Nationwide': 'national',
    'Statewide 1+': 'statewide',
}

TRIBAL_NAME_MAP = {
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
    'Red Lake Nation': 'Red Lake',
    'Leech Lake Band': 'Leech Lake',
    'White Earth Nation': 'White Earth',
    'Mille Lacs Band': 'Mille Lacs',
    'Fond du Lac Band': 'Fond du Lac',
    'Grand Portage Band': 'Grand Portage',
    'Bois Forte Band': 'Bois Forte',
}

NETWORK_FIELDS = [
    'Do you have ties to any local networks or initiatives? Select all that apply',
    'Do you have ties to any nationwide networks or initiatives? Select all that apply',
    'Other Relevant Networks or Initiatives',
]

def normalize_tribal_name(name):
    return TRIBAL_NAME_MAP.get(name.strip(), name.strip())

def parse_network_field(val):
    if not val or not val.strip():
        return []
    reader = csv.reader(io.StringIO(val))
    return [n.strip() for n in next(reader) if n.strip()]

def fetch_all_records():
    """Fetch all records from Airtable, handling pagination."""
    records = []
    offset = None
    table_encoded = urllib.parse.quote(TABLE_NAME)
    
    while True:
        url = f'https://api.airtable.com/v0/{BASE_ID}/{table_encoded}'
        if offset:
            url += f'?offset={offset}'
        
        req = urllib.request.Request(url, headers={
            'Authorization': f'Bearer {AIRTABLE_API_KEY}',
            'Content-Type': 'application/json',
        })
        
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())
        
        records.extend(data.get('records', []))
        offset = data.get('offset')
        if not offset:
            break
        print(f'  Fetched {len(records)} records so far...')
    
    return records

def convert_record(record, idx):
    """Convert a single Airtable record to an org dict."""
    fields = record.get('fields', {})
    
    name = fields.get('Organization Name', '').strip()
    if not name:
        return None

    scope_raw = fields.get('Where does your organization primarily work?', '').strip()
    scope = SCOPE_MAP.get(scope_raw, 'local')

    # County FIPS
    fips_list = []
    for field, val in fields.items():
        if ('Counties' in field or 'County' in field) and val:
            if isinstance(val, list):
                counties = val
            else:
                counties = [c.strip() for c in str(val).split(',') if c.strip()]
            
            state_name = field.replace(' Counties (select all that apply)', '').replace(' Counties', '').strip()
            state = us.states.lookup(state_name)
            if not state:
                continue
            for county in counties:
                fips_str = af.get_county_fips(county.strip(), state=state.abbr)
                if fips_str:
                    fips_list.append(int(fips_str))

    # Tribal nations
    tribal_data = []
    for field, val in fields.items():
        if 'Tribal' in field and val:
            nations = val if isinstance(val, list) else [n.strip() for n in str(val).split(',') if n.strip()]
            for nation in nations:
                tribal_data.append(normalize_tribal_name(nation))

    # Networks
    networks = []
    for field in NETWORK_FIELDS:
        val = fields.get(field, '')
        if val:
            if isinstance(val, list):
                networks.extend(val)
            else:
                networks.extend(parse_network_field(str(val)))

    # States for statewide orgs
    states_list = []
    if scope == 'statewide':
        state_val = fields.get('What state(s)? Local', '')
        if state_val:
            if isinstance(state_val, list):
                states_list = state_val
            else:
                states_list = [s.strip() for s in str(state_val).split(',') if s.strip()]

    org = {
        'id': idx,
        'name': name,
        'website': fields.get('Organization Website', '').strip(),
        'scope': scope,
        'city': fields.get('City', '').strip(),
        'hqState': fields.get('State', '').strip(),
        'mission': '',
        'fips': fips_list,
        'states': states_list,
        'networks': networks,
    }
    if tribal_data:
        org['tribalNations'] = tribal_data

    return org

def main(output_path='orgs.json'):
    if not AIRTABLE_API_KEY:
        print('ERROR: AIRTABLE_API_KEY environment variable not set')
        sys.exit(1)

    print(f'Fetching records from Airtable...')
    records = fetch_all_records()
    print(f'Fetched {len(records)} total records')

    orgs = []
    for i, record in enumerate(records):
        org = convert_record(record, i)
        if org:
            orgs.append(org)

    with open(output_path, 'w') as f:
        json.dump(orgs, f, indent=2)

    print(f'✓ Converted {len(orgs)} organizations → {output_path}')

if __name__ == '__main__':
    output = 'orgs.json'
    if '--output' in sys.argv:
        output = sys.argv[sys.argv.index('--output') + 1]
    main(output)
