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

import os, sys, json, re, us, addfips, urllib.request, urllib.parse

AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
BASE_ID = 'app0PiJ1y0rvCPBXL'
TABLE_NAME = 'Networks Base'

af = addfips.AddFIPS()

STATE_FIPS_PREFIX = {s.abbr: int(s.fips) for s in us.states.STATES}
STATE_FIPS_PREFIX['DC'] = 11

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

# Known county centroids (FIPS int -> (lng, lat))
COUNTY_CENTROIDS = {
    6075: (-122.44, 37.76), 6037: (-118.24, 34.05), 6073: (-117.11, 32.72),
    6059: (-117.83, 33.70), 6065: (-116.20, 33.74), 6071: (-116.18, 34.84),
    6013: (-122.05, 37.85), 6071: (-116.18, 34.84),
    53033: (-122.12, 47.49), 53061: (-122.15, 48.05), 53057: (-122.33, 48.48),
    53073: (-122.40, 48.75), 53031: (-122.63, 48.53), 53055: (-123.05, 48.59),
    53007: (-120.65, 47.49), 53017: (-119.70, 47.73), 53025: (-119.45, 47.21),
    53047: (-119.74, 48.35), 37067: (-80.25, 36.10),
    13121: (-84.39, 33.77), 13089: (-84.23, 33.77), 13067: (-84.58, 33.90),
    13135: (-84.01, 33.65), 13063: (-84.35, 33.54),
    39061: (-84.54, 39.10), 36063: (-79.05, 43.10),
    42101: (-75.13, 40.00), 42077: (-75.36, 40.61), 42095: (-75.48, 40.69),
    34021: (-74.66, 40.28), 12099: (-80.10, 26.65), 4021: (-111.37, 32.89),
    55139: (-88.54, 44.26), 55087: (-88.42, 44.26), 55015: (-88.22, 44.49),
    26017: (-84.07, 43.62), 26073: (-84.36, 44.01), 26111: (-84.13, 43.95),
    26145: (-83.86, 43.44), 26001: (-83.74, 44.32), 26051: (-84.49, 43.67),
    26069: (-84.13, 44.33), 26099: (-84.53, 43.60),
}

def get_county_centroid(fips_int):
    if fips_int in COUNTY_CENTROIDS:
        return COUNTY_CENTROIDS[fips_int]
    state_prefix = fips_int // 1000
    county_code = fips_int % 1000
    state_abbr = next((k for k, v in STATE_FIPS_PREFIX.items() if v == state_prefix), None)
    if state_abbr and state_abbr in STATE_CENTROIDS:
        base_lng, base_lat = STATE_CENTROIDS[state_abbr]
        offset = (county_code / 1000) * 2 - 1
        return (base_lng + offset * 0.5, base_lat + offset * 0.3)
    return None

def parse_geography(geo_str):
    """
    Parse the Geography field into (fips_list, state_abbrs, lng, lat).
    Handles formats like:
      - 'King County, WA; Snohomish County, WA'
      - 'WA, OR, ID'
      - 'GA'
    """
    if not geo_str or not geo_str.strip():
        return [], [], None, None

    fips_list = []
    state_abbrs = []
    all_states = {s.abbr for s in us.states.STATES} | {'DC'}

    # Split on semicolons first, then handle comma-separated states within each part
    parts = [p.strip() for p in re.split(r';', geo_str) if p.strip()]
    expanded = []
    for part in parts:
        # Check if it's 'County Name, ST' (one comma with 2-letter state at end)
        m = re.match(r'^(.+),\s*([A-Z]{2})$', part)
        if m and m.group(2) in all_states:
            expanded.append(part)
        else:
            # Comma-separated state abbreviations
            for sub in [s.strip() for s in part.split(',') if s.strip()]:
                expanded.append(sub)

    for part in expanded:
        m = re.match(r'^(.+),\s*([A-Z]{2})$', part)
        if m and m.group(2) in all_states:
            county = m.group(1).strip()
            state = m.group(2).strip()
            fips_str = af.get_county_fips(county, state=state)
            if fips_str:
                fips_list.append(int(fips_str))
            if state not in state_abbrs:
                state_abbrs.append(state)
        elif part in all_states:
            if part not in state_abbrs:
                state_abbrs.append(part)

    # Compute centroid from county FIPS if available
    if fips_list:
        coords = [get_county_centroid(f) for f in fips_list if get_county_centroid(f)]
        if coords:
            lng = round(sum(c[0] for c in coords) / len(coords), 4)
            lat = round(sum(c[1] for c in coords) / len(coords), 4)
            # Return multiple pins if counties span multiple states
            pins = []
            by_state = {}
            for f in fips_list:
                state_prefix = f // 1000
                sa = next((k for k, v in STATE_FIPS_PREFIX.items() if v == state_prefix), None)
                if sa:
                    by_state.setdefault(sa, []).append(f)
            if len(by_state) > 1:
                for sa, fips in by_state.items():
                    c2 = [get_county_centroid(f) for f in fips if get_county_centroid(f)]
                    if c2:
                        pins.append({
                            'lng': round(sum(x[0] for x in c2) / len(c2), 4),
                            'lat': round(sum(x[1] for x in c2) / len(c2), 4),
                        })
            return fips_list, state_abbrs, lng, lat, pins if len(pins) > 1 else []

    # Each state gets its own pin
    state_coords = [(a, STATE_CENTROIDS[a]) for a in state_abbrs if a in STATE_CENTROIDS]
    if state_coords:
        # Primary pin = first state
        lng, lat = state_coords[0][1]
        pins = [{'lng': c[0], 'lat': c[1]} for _, c in state_coords]
        return fips_list, state_abbrs, round(lng, 4), round(lat, 4), pins if len(pins) > 1 else []

    return fips_list, state_abbrs, None, None, []

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

    return records

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
        'id': int(fields['Id']) if fields.get('Id') else None,
        'name': name,
        'website': (fields.get('Network Website') or '').strip(),
        'scale': scale,
        'fips': [],
        'states': [],
        'lng': None,
        'lat': None,
    }

    if scale == 'nationwide':
        pass

    else:
        geo_str = fields.get(
            'Geography (WA or MA; King County, WA; Snohomish County, WA)', ''
        ) or ''
        fips_list, state_abbrs, lng, lat, pins = parse_geography(geo_str)
        network['fips'] = fips_list
        network['states'] = state_abbrs
        network['lng'] = lng
        network['lat'] = lat
        if pins:
            network['pins'] = pins

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
