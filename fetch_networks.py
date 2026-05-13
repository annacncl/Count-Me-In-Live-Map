#!/usr/bin/env python3
"""
fetch_networks.py
-----------------
Fetches network records from Airtable and converts them to networks.json
for the Count-Me-In Live Map.

Usage:
    AIRTABLE_API_KEY=your_key python3 fetch_networks.py
    python3 fetch_networks.py --output networks.json
"""

import os, sys, json, us, urllib.request, urllib.parse

AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
BASE_ID = 'app0PiJ1y0rvCPBXL'
TABLE_NAME = 'Networks Base'

STATE_CENTROIDS = {
    'AL': (-86.79, 32.80), 'AK': (-152.40, 64.20), 'AZ': (-111.09, 34.05),
    'AR': (-92.37, 34.75), 'CA': (-119.68, 37.27), 'CO': (-105.55, 39.00),
    'CT': (-72.65, 41.60), 'DE': (-75.52, 38.99), 'FL': (-81.52, 27.77),
    'GA': (-83.44, 32.68), 'HI': (-155.58, 19.90), 'ID': (-114.48, 44.35),
    'IL': (-88.99, 40.04), 'IN': (-86.13, 40.27), 'IA': (-93.21, 42.01),
    'KS': (-98.38, 38.53), 'KY': (-84.86, 37.84), 'LA': (-91.96, 31.17),
    'ME': (-69.38, 45.37), 'MD': (-76.80, 39.06), 'MA': (-71.53, 42.23),
    'MI': (-84.71, 44.35), 'MN': (-94.34, 46.41), 'MS': (-89.66, 32.74),
    'MO': (-92.46, 38.46), 'MT': (-109.64, 46.88), 'NE': (-99.90, 41.49),
    'NV': (-116.42, 38.50), 'NH': (-71.57, 43.19), 'NJ': (-74.67, 40.14),
    'NM': (-106.11, 34.31), 'NY': (-75.53, 42.94), 'NC': (-79.39, 35.56),
    'ND': (-100.47, 47.44), 'OH': (-82.91, 40.37), 'OK': (-97.52, 35.47),
    'OR': (-120.55, 43.94), 'PA': (-77.21, 40.89), 'RI': (-71.51, 41.70),
    'SC': (-80.90, 33.84), 'SD': (-100.23, 44.44), 'TN': (-86.35, 35.86),
    'TX': (-99.33, 31.47), 'UT': (-111.09, 39.32), 'VT': (-72.71, 44.05),
    'VA': (-78.45, 37.43), 'WA': (-120.74, 47.40), 'WV': (-80.62, 38.64),
    'WI': (-89.62, 44.27), 'WY': (-107.55, 43.00), 'DC': (-77.03, 38.90),
}

def fetch_all_records():
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

def get_state_abbrs(val):
    """Parse state abbreviations from a comma-separated string."""
    if not val:
        return []
    if isinstance(val, list):
        return val
    return [s.strip() for s in str(val).split(',') if s.strip()]

def convert_record(record):
    fields = record.get('fields', {})

    name = fields.get('Network Name', '').strip()
    if not name:
        return None

    scale_raw = fields.get('Network Scale', '').strip()
    if scale_raw == 'Local':
        scale = 'local'
    elif scale_raw == 'Statewide':
        scale = 'statewide'
    elif scale_raw in ('Nationwide', 'National'):
        scale = 'nationwide'
    else:
        return None

    network = {
        'id': int(fields.get('Id', 0)) if fields.get('Id') else None,
        'name': name,
        'website': fields.get('Network Website', '').strip() if fields.get('Network Website') else '',
        'scale': scale,
        'fips': [],
        'states': [],
        'lng': None,
        'lat': None,
    }

    if scale == 'nationwide':
        pass  # no geo needed

    elif scale in ('statewide', 'local'):
        state_val = fields.get('What state(s)? Local', '')
        abbrs = get_state_abbrs(state_val)
        network['states'] = abbrs
        # Pin at first state's centroid
        if abbrs and abbrs[0] in STATE_CENTROIDS:
            network['lng'], network['lat'] = STATE_CENTROIDS[abbrs[0]]

    return network

def main(output_path='networks.json'):
    if not AIRTABLE_API_KEY:
        print('ERROR: AIRTABLE_API_KEY environment variable not set')
        sys.exit(1)

    print('Fetching networks from Airtable...')
    records = fetch_all_records()
    print(f'Fetched {len(records)} total records')

    networks = []
    for record in records:
        net = convert_record(record)
        if net:
            networks.append(net)

    with open(output_path, 'w') as f:
        json.dump(networks, f, indent=2)

    print(f'✓ Converted {len(networks)} networks → {output_path}')
    print(f'  Nationwide: {sum(1 for n in networks if n["scale"] == "nationwide")}')
    print(f'  Statewide:  {sum(1 for n in networks if n["scale"] == "statewide")}')
    print(f'  Local:      {sum(1 for n in networks if n["scale"] == "local")}')

if __name__ == '__main__':
    output = 'networks.json'
    if '--output' in sys.argv:
        output = sys.argv[sys.argv.index('--output') + 1]
    main(output)
