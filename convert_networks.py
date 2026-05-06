"""
convert_networks.py
-------------------
Converts the Rippel Networks Airtable CSV export into networks.json
for use by the Rippel ecosystem map prototype.

Usage:
    python3 convert_networks.py input.csv output.json

For each local/statewide network, computes a centroid lat/long
based on the counties or states selected. Nationwide networks
are included in the output but have no coordinates (they appear
in the right panel, not on the map).
"""

import sys
import json
import pandas as pd
import addfips
import us

af = addfips.AddFIPS()

# State abbr -> FIPS prefix
STATE_FIPS_PREFIX = {s.abbr: int(s.fips) for s in us.states.STATES}
STATE_FIPS_PREFIX['DC'] = 11

# Approximate county centroids — we use the average of well-known
# county center coordinates. For simplicity we use state centroids
# as fallback and county centroids where we can compute them.

# State centroids (lng, lat) for statewide network pin placement
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

# County approximate centroids — keyed by FIPS int
# We compute these on the fly using a rough formula:
# state centroid + small offset based on county FIPS position
# For production, replace with actual Census TIGER centroids

COUNTY_COL_TO_STATE = {
    'Alabama Counties': 'Alabama',
    'Arizona Counties (select all that apply)': 'Arizona',
    'Alaska Counties (select all that apply)': 'Alaska',
    'California Counties (select all that apply)': 'California',
    'Colorado Counties': 'Colorado',
    'Connecticut Counties (select all that apply)': 'Connecticut',
    'Delaware (select all that apply)': 'Delaware',
    'Florida Counties (select all that apply)': 'Florida',
    'Georgia Counties (select all that apply)': 'Georgia',
    'Hawaii Counties (select all that apply)': 'Hawaii',
    'Idaho Counties': 'Idaho',
    'Illinois Counties': 'Illinois',
    'Indiana Counties': 'Indiana',
    'Iowa Counties': 'Iowa',
    'Kansas Counties': 'Kansas',
    'Kentucky Counties': 'Kentucky',
    'Louisiana Counties': 'Louisiana',
    'Maine Counties': 'Maine',
    'Maryland Counties': 'Maryland',
    'Massachusetts Counties': 'Massachusetts',
    'Michigan Counties': 'Michigan',
    'Minnesota Counties': 'Minnesota',
    'Mississippi Counties': 'Mississippi',
    'Missouri Counties (select all that apply)': 'Missouri',
    'Montana Counties (select all that apply)': 'Montana',
    'Nebraska Counties (select all that apply)': 'Nebraska',
    'Nevada Counties (select all that apply)': 'Nevada',
    'New Hampshire Counties (select all that apply)': 'New Hampshire',
    'New Jersey Counties (select all that apply)': 'New Jersey',
    'New Mexico Counties (select all that apply)': 'New Mexico',
    'New York Counties (select all that apply)': 'New York',
    'North Carolina Counties (select all that apply)': 'North Carolina',
    'North Dakota Counties (select all that apply)': 'North Dakota',
    'Ohio Counties (select all that apply)': 'Ohio',
    'Oklahoma Counties (select all that apply)': 'Oklahoma',
    'Oregon Counties (select all that apply)': 'Oregon',
    'Pennsylvania Counties (select all that apply)': 'Pennsylvania',
    'Rhode Island Counties (select all that apply)': 'Rhode Island',
    'South Carolina Counties (select all that apply)': 'South Carolina',
    'South Dakota Counties (select all that apply)': 'South Dakota',
    'Tennessee Counties (select all that apply)': 'Tennessee',
    'Texas Counties': 'Texas',
    'Utah Counties (select all that apply)': 'Utah',
    'Vermont Counties (select all that apply)': 'Vermont',
    'Virginia Counties (select all that apply)': 'Virginia',
    'Washington Counties (select all that apply)': 'Washington',
    'West Virginia Counties (select all that apply)': 'West Virginia',
    'Wisconsin Counties (select all that apply)': 'Wisconsin',
    'Wyoming Counties (select all that apply)': 'Wyoming',
}

COUNTY_NAME_OVERRIDES = {
    ('Dade County', 'Florida'): 'Miami-Dade',
    ('Dade', 'Florida'): 'Miami-Dade',
}

# Approximate county centroids from Census (lng, lat) — key counties
# This is a representative set; expand as needed
COUNTY_CENTROIDS = {
    6075: (-122.44, 37.76),  # San Francisco, CA
    6037: (-118.24, 34.05),  # Los Angeles, CA
    6073: (-117.11, 32.72),  # San Diego, CA
    6059: (-117.83, 33.70),  # Orange, CA
    6065: (-116.20, 33.74),  # Riverside, CA
    6071: (-116.18, 34.84),  # San Bernardino, CA
    6013: (-122.05, 37.85),  # Contra Costa, CA
    53033: (-122.12, 47.49), # King, WA
    53061: (-122.15, 48.05), # Snohomish, WA
    53057: (-122.33, 48.48), # Skagit, WA
    53073: (-122.40, 48.75), # Whatcom, WA
    53031: (-122.63, 48.53), # Island, WA
    53055: (-48.59, 122.80), # San Juan, WA -- corrected below
    53007: (-120.65, 47.49), # Chelan, WA
    53017: (-119.70, 47.73), # Douglas, WA
    53025: (-119.45, 47.21), # Grant, WA
    53047: (-119.74, 48.35), # Okanogan, WA
    37067: (-80.25, 36.10),  # Forsyth, NC
    13121: (-84.39, 33.77),  # Fulton, GA
    13089: (-84.23, 33.77),  # DeKalb, GA
    13067: (-84.58, 33.90),  # Cobb, GA
    13135: (-84.01, 33.65),  # Gwinnett, GA
    13063: (-84.35, 33.54),  # Clayton, GA
    39061: (-84.54, 39.10),  # Hamilton, OH
    36063: (-79.05, 43.10),  # Niagara, NY
    42101: (-75.13, 40.00),  # Philadelphia, PA
    34021: (-74.66, 40.28),  # Mercer, NJ
    12099: (-80.10, 26.65),  # Palm Beach, FL
    4021: (-111.37, 32.89),  # Pinal, AZ
    55139: (-88.54, 44.26),  # Winnebago, WI
    55087: (-88.54, 44.26),  # Outagamie, WI
    55015: (-88.22, 44.49),  # Calumet, WI
    26017: (-84.07, 43.62),  # Bay, MI
    26073: (-84.36, 44.01),  # Isabella, MI
    26111: (-84.13, 43.95),  # Midland, MI
    26145: (-83.86, 43.44),  # Saginaw, MI
}
# Fix San Juan WA
COUNTY_CENTROIDS[53055] = (-123.05, 48.59)


def get_fips(county_raw, state_name):
    county_raw = county_raw.strip()
    override = COUNTY_NAME_OVERRIDES.get((county_raw, state_name))
    if override:
        county_raw = override
    fips = af.get_county_fips(county_raw, state=state_name)
    if fips:
        return int(fips)
    for suffix in [' County', ' Parish', ' Borough', ' Census Area', ' Municipality']:
        if county_raw.endswith(suffix):
            cleaned = county_raw[:-len(suffix)]
            fips = af.get_county_fips(cleaned, state=state_name)
            if fips:
                return int(fips)
    return None


def get_county_centroid(fips_int):
    """Get approximate centroid for a county FIPS code."""
    if fips_int in COUNTY_CENTROIDS:
        return COUNTY_CENTROIDS[fips_int]
    # Fall back to state centroid with small jitter based on county code
    state_prefix = fips_int // 1000
    county_code = fips_int % 1000
    state_abbr = next((k for k, v in STATE_FIPS_PREFIX.items() if v == state_prefix), None)
    if state_abbr and state_abbr in STATE_CENTROIDS:
        base_lng, base_lat = STATE_CENTROIDS[state_abbr]
        # Small offset so pins don't all stack on state center
        offset = (county_code / 1000) * 2 - 1
        return (base_lng + offset * 0.5, base_lat + offset * 0.3)
    return None


def compute_centroid(fips_list):
    """Average centroid of a list of county FIPS codes."""
    coords = [get_county_centroid(f) for f in fips_list if get_county_centroid(f)]
    if not coords:
        return None, None
    avg_lng = sum(c[0] for c in coords) / len(coords)
    avg_lat = sum(c[1] for c in coords) / len(coords)
    return round(avg_lng, 4), round(avg_lat, 4)


def convert(input_path, output_path):
    df = pd.read_csv(input_path)
    networks = []
    failed = []

    for _, row in df.iterrows():
        name = str(row.get('Network Name', '')).strip()
        if not name or name == 'nan':
            continue

        scale_raw = str(row.get('Network Scale', '')).strip()
        if scale_raw == 'Local':
            scale = 'local'
        elif scale_raw == 'Statewide':
            scale = 'statewide'
        elif scale_raw in ('Nationwide', 'National'):
            scale = 'nationwide'
        else:
            continue

        network = {
            'id': int(row['Id']) if 'Id' in row and not pd.isna(row['Id']) else None,
            'name': name,
            'website': str(row.get('Network Website', '')).strip() if not pd.isna(row.get('Network Website', '')) else '',
            'scale': scale,
            'fips': [],
            'states': [],
            'lng': None,
            'lat': None,
        }

        if scale == 'nationwide':
            pass  # no geo needed

        elif scale == 'statewide':
            states_raw = str(row.get('What state(s)? Local', '')).strip()
            if states_raw and states_raw != 'nan':
                abbrs = [a.strip() for a in states_raw.split(',') if a.strip()]
                network['states'] = abbrs
                # Pin at first state's centroid
                if abbrs and abbrs[0] in STATE_CENTROIDS:
                    network['lng'], network['lat'] = STATE_CENTROIDS[abbrs[0]]

        elif scale == 'local':
            all_fips = []
            for col, state_name in COUNTY_COL_TO_STATE.items():
                val = row.get(col)
                if pd.isna(val) or str(val).strip() == '':
                    continue
                county_names = [c.strip() for c in str(val).split(',') if c.strip()]
                for county in county_names:
                    fips = get_fips(county, state_name)
                    if fips:
                        all_fips.append(fips)
                    else:
                        failed.append({'network': name, 'county': county, 'state': state_name})

            # If no counties found, try state centroid
            if not all_fips:
                states_raw = str(row.get('What state(s)? Local', '')).strip()
                if states_raw and states_raw != 'nan':
                    abbrs = [a.strip() for a in states_raw.split(',') if a.strip()]
                    network['states'] = abbrs
                    if abbrs and abbrs[0] in STATE_CENTROIDS:
                        network['lng'], network['lat'] = STATE_CENTROIDS[abbrs[0]]
            else:
                network['fips'] = list(set(all_fips))
                lng, lat = compute_centroid(all_fips)
                network['lng'] = lng
                network['lat'] = lat

        networks.append(network)

    with open(output_path, 'w') as f:
        json.dump(networks, f, indent=2)

    print(f"✓ Processed {len(networks)} networks")
    print(f"  Nationwide: {sum(1 for n in networks if n['scale']=='nationwide')}")
    print(f"  Statewide:  {sum(1 for n in networks if n['scale']=='statewide')}")
    print(f"  Local:      {sum(1 for n in networks if n['scale']=='local')}")
    print()
    if failed:
        print(f"⚠ {len(failed)} county name(s) could not be matched:")
        for f in failed:
            print(f"  [{f['state']}] {f['county']} — network: {f['network']}")
    else:
        print("✓ All county names matched successfully.")
    print(f"\n→ Output written to: {output_path}")
    return networks


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python3 convert_networks.py input.csv output.json")
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
