#!/usr/bin/env python3
"""
process_school_data.py — Data preparation for U.S. School Closure Risk Map

Processes:
1. CCD school directory/enrollment data (from Urban Institute API)
2. Hauer county-level population projections (SSP2, ages 5-17)
3. Census Bureau national projections distributed to counties
4. Brookings TPS share scenarios

Outputs: school_risk_data.json

Usage:
    python process_school_data.py
"""

import csv
import io
import json
import os
import sys
import time
import zipfile
import urllib.request
import urllib.error
from collections import defaultdict
from pathlib import Path

try:
    import openpyxl
except ImportError:
    print("ERROR: openpyxl required. Install with: pip install openpyxl")
    sys.exit(1)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR

HAUER_CSV = DATA_DIR / "SSP_asrc.csv"
HAUER_ZIP_ALT = DATA_DIR / "SSP_asrc.zip"
CENSUS_XLSX = DATA_DIR / "np2023-t2 (1).xlsx"
BROOKINGS_PARAMS = BASE_DIR / "brookings_params.json"
CCD_CACHE = DATA_DIR / "ccd_schools_2022.json"
ACS_CACHE = DATA_DIR / "acs_county_school_age.json"

YEARS = [2025, 2030, 2035, 2040, 2045, 2050]

STATE_FIPS = {
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56"
}

STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY"
}


# ---------------------------------------------------------------
# Step 1: Fetch CCD school data from Urban Institute API
# ---------------------------------------------------------------
def fetch_ccd_schools():
    """Fetch CCD school directory data from Urban Institute Education Data Portal."""
    if CCD_CACHE.exists():
        print("  Using cached CCD data from data/ccd_schools_2022.json")
        with open(CCD_CACHE) as f:
            return json.load(f)

    print("  Fetching CCD school data from Urban Institute API...")
    print("  (This will make ~11 API requests, please be patient)")

    all_schools = []
    base_url = "https://educationdata.urban.org/api/v1/schools/ccd/directory/2022/"
    page = 1
    while True:
        url = f"{base_url}?limit=10000&page={page}"
        print(f"    Fetching page {page}...")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "school-closure-risk-map/1.0"})
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            print(f"    ERROR on page {page}: {e}")
            break

        results = data.get("results", [])
        if not results:
            break
        all_schools.extend(results)
        print(f"    Got {len(results)} schools (total: {len(all_schools)})")

        if data.get("next") is None:
            break
        page += 1
        time.sleep(0.5)  # Be polite to the API

    # Cache for future runs
    with open(CCD_CACHE, "w") as f:
        json.dump(all_schools, f)
    print(f"  Cached {len(all_schools)} schools to {CCD_CACHE}")
    return all_schools


def filter_tps_schools(all_schools):
    """Filter to open, regular, brick-and-mortar traditional public schools."""
    filtered = []
    for s in all_schools:
        # school_status: 1=open, 2=closed, etc.
        if s.get("school_status") != 1:
            continue
        # school_type: 1=regular, 2=special ed, 3=vocational, 4=alternative
        if s.get("school_type") != 1:
            continue
        # Exclude charter schools
        if s.get("charter") == 1:
            continue
        # Exclude virtual schools
        if s.get("virtual") in (1, 2):  # 1=fully virtual, 2=virtual with face-to-face
            continue
        # Need valid enrollment
        enrollment = s.get("enrollment")
        if enrollment is None or enrollment <= 0:
            continue
        # Need valid county code
        county_code = s.get("county_code")
        if not county_code:
            continue

        # Build 5-digit FIPS
        cc = str(county_code).zfill(5)
        state_fips = cc[:2]
        if state_fips not in STATE_FIPS:
            continue

        filtered.append({
            "ncessch": s.get("ncessch"),
            "name": s.get("school_name", ""),
            "fips": cc,
            "state_fips": state_fips,
            "enrollment": enrollment,
            "lat": s.get("latitude"),
            "lon": s.get("longitude"),
        })

    return filtered


# ---------------------------------------------------------------
# Step 2: Hauer school-age projections (ages 5-17)
# ---------------------------------------------------------------
def process_hauer_school_age():
    """Extract school-age (5-17) population projections from Hauer SSP2 data."""
    print("\n[2/5] Processing Hauer school-age projections...")

    csv_path = HAUER_CSV
    if not csv_path.exists():
        zip_path = HAUER_ZIP_ALT if HAUER_ZIP_ALT.exists() else None
        if zip_path:
            print(f"  Extracting {zip_path.name}...")
            import zipfile
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(DATA_DIR)

    if not csv_path.exists():
        print("  ERROR: Cannot find SSP_asrc.csv. Run process_data.py first.")
        sys.exit(1)

    # AGE groups 1-18: 1=0-4, 2=5-9, 3=10-14, 4=15-19 (proxy for 14-17)
    # School age (5-17) ≈ AGE groups 2 (5-9), 3 (10-14), and part of 4 (15-19)
    # We'll use groups 2, 3, 4 as a proxy (ages 5-19), which is standard for school-age
    # Actually, for consistency with Census 5-17, we use groups 2 (5-9) and 3 (10-14)
    # plus ~60% of group 4 (15-19) to approximate 15-17.
    # Simpler: use groups 2+3+4 as "school-age" (5-19) — slightly overestimates but consistent

    county_data = defaultdict(lambda: defaultdict(lambda: {"school_age": 0.0, "total": 0.0}))

    row_count = 0
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            reader.fieldnames = [fn.strip() for fn in reader.fieldnames]

        for row in reader:
            row_count += 1
            if row_count % 5000000 == 0:
                print(f"    ...processed {row_count:,} rows")

            year = int(row.get("YEAR", "0"))
            if year not in [2020] + YEARS:
                continue

            state_fips = row.get("STATE", "").strip().zfill(2)
            county_fips = row.get("COUNTY", "").strip().zfill(3)
            if state_fips not in STATE_FIPS:
                continue

            fips = state_fips + county_fips
            age_group = int(row.get("AGE", "0"))
            value = float(row.get("SSP2", "0"))

            county_data[fips][year]["total"] += value
            # School-age: groups 2 (5-9), 3 (10-14), 4 (15-19)
            if age_group in (2, 3, 4):
                county_data[fips][year]["school_age"] += value

    print(f"  Processed {row_count:,} rows, found {len(county_data)} counties")
    return county_data


# ---------------------------------------------------------------
# Step 3: Census national school-age projections → county distribution
# ---------------------------------------------------------------
def fetch_acs_school_age():
    """Fetch ACS county-level school-age (5-17) population from Census API."""
    if ACS_CACHE.exists():
        print("  Using cached ACS school-age data")
        with open(ACS_CACHE) as f:
            return json.load(f)

    print("  Fetching ACS school-age data from Census API...")

    # B01001: Sex by Age. School-age (5-17):
    # Male: B01001_004E (5-9), B01001_005E (10-14), B01001_006E (15-17)
    # Female: B01001_028E (5-9), B01001_029E (10-14), B01001_030E (15-17)
    school_age_vars = [
        "B01001_004E", "B01001_005E", "B01001_006E",
        "B01001_028E", "B01001_029E", "B01001_030E"
    ]
    all_vars = ["NAME", "B01001_001E"] + school_age_vars
    var_str = ",".join(all_vars)

    url = f"https://api.census.gov/data/2022/acs/acs5?get={var_str}&for=county:*&in=state:*"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "school-closure-risk-map/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"  ERROR fetching ACS data: {e}")
        sys.exit(1)

    headers = data[0]
    rows = data[1:]

    county_age = {}
    for row in rows:
        record = dict(zip(headers, row))
        state = record["state"]
        county = record["county"]
        if state not in STATE_FIPS:
            continue

        fips = state + county
        total_pop = int(record.get("B01001_001E", 0) or 0)
        school_age = sum(int(record.get(v, 0) or 0) for v in school_age_vars)

        county_age[fips] = {
            "name": record.get("NAME", fips),
            "state_fips": state,
            "pop_total": total_pop,
            "school_age": school_age,
        }

    with open(ACS_CACHE, "w") as f:
        json.dump(county_age, f)
    print(f"  Fetched ACS data for {len(county_age)} counties")
    return county_age


def process_census_school_age():
    """Process Census national projections and distribute to counties."""
    print("\n[3/5] Processing Census school-age projections...")

    # Find the Excel file
    census_path = CENSUS_XLSX
    if not census_path.exists():
        # Try alternative locations
        alt_paths = [
            DATA_DIR / "np2023-t2.xlsx",
            Path.home() / "Downloads" / "np2023-t2 (1).xlsx",
            Path.home() / "Downloads" / "np2023-t2.xlsx",
        ]
        for p in alt_paths:
            if p.exists():
                census_path = p
                break

    if not census_path.exists():
        print(f"  WARNING: Census projections file not found at {CENSUS_XLSX}")
        print("  Will use Hauer projections only for Census scenario.")
        return None

    wb = openpyxl.load_workbook(str(census_path), read_only=True, data_only=True)
    ws = wb["Main series (thousands)"]
    rows = list(ws.iter_rows(values_only=True))

    # Years from row 5
    year_row = rows[5]
    years_avail = [int(y) for y in year_row[1:] if y is not None and str(y).strip() not in ("", " ")]

    def get_val(row_idx, year_idx):
        try:
            return float(rows[row_idx][1 + year_idx])
        except:
            return 0

    # School-age: Under 18 (row 7) minus Under 5 (row 8) = ages 5-17
    # Total: row 6
    national_projections = {}
    for i, yr in enumerate(years_avail):
        total = get_val(6, i)
        under18 = get_val(7, i)
        under5 = get_val(8, i)
        school_age = under18 - under5  # ages 5-17
        national_projections[yr] = {
            "total": total,
            "school_age": school_age,
        }
    wb.close()

    print(f"  National projections years: {sorted(national_projections.keys())}")
    for yr in YEARS:
        if yr in national_projections:
            np = national_projections[yr]
            print(f"    {yr}: school-age = {np['school_age']:.0f}K")

    # Fetch county shares
    county_acs = fetch_acs_school_age()

    # Compute state totals for school-age
    state_totals = defaultdict(lambda: {"school_age": 0, "total": 0})
    for fips, c in county_acs.items():
        sf = c["state_fips"]
        state_totals[sf]["school_age"] += c["school_age"]
        state_totals[sf]["total"] += c["pop_total"]

    # National total school-age from ACS
    national_acs_school_age = sum(c["school_age"] for c in county_acs.values())

    # Distribute national projections to counties using county share of national school-age
    county_projections = {}
    for fips, c in county_acs.items():
        share = c["school_age"] / national_acs_school_age if national_acs_school_age > 0 else 0

        proj = {}
        # 2020 baseline from ACS
        proj[2020] = c["school_age"]

        for yr in YEARS:
            if yr in national_projections:
                # National school-age in thousands → actual
                nat_sa = national_projections[yr]["school_age"] * 1000
                proj[yr] = nat_sa * share
            else:
                # Interpolate
                avail = sorted(national_projections.keys())
                before = [y for y in avail if y <= yr]
                after = [y for y in avail if y >= yr]
                if before and after:
                    y1, y2 = before[-1], after[0]
                    if y1 == y2:
                        nat_sa = national_projections[y1]["school_age"] * 1000
                    else:
                        t = (yr - y1) / (y2 - y1)
                        sa1 = national_projections[y1]["school_age"] * 1000
                        sa2 = national_projections[y2]["school_age"] * 1000
                        nat_sa = sa1 + t * (sa2 - sa1)
                    proj[yr] = nat_sa * share

        county_projections[fips] = {
            "name": c["name"],
            "baseline_school_age": c["school_age"],
            "projections": proj,
        }

    print(f"  Distributed to {len(county_projections)} counties")
    return county_projections


# ---------------------------------------------------------------
# Step 4: Compute TPS share multipliers from Brookings scenarios
# ---------------------------------------------------------------
def compute_tps_multipliers():
    """Compute TPS share multipliers for each Brookings scenario."""
    print("\n[4/5] Computing TPS share multipliers...")

    with open(BROOKINGS_PARAMS) as f:
        params = json.load(f)

    hist = params["tps_share_data"]["historical_enrollment_thousands"]

    # Compute TPS share = TPS / population for each year
    pre_years = ["2015-16", "2016-17", "2017-18", "2018-19", "2019-20"]
    post_years = ["2020-21", "2021-22", "2022-23", "2023-24"]

    pre_shares = []
    for yr in pre_years:
        d = hist[yr]
        pre_shares.append(d["tps"] / d["population"])

    post_shares = []
    for yr in post_years:
        d = hist[yr]
        post_shares.append(d["tps"] / d["population"])

    # Pre-pandemic average TPS share
    pre_avg = sum(pre_shares) / len(pre_shares)
    # Post-pandemic average TPS share
    post_avg = sum(post_shares) / len(post_shares)

    # Pre-pandemic trend: linear fit
    # Use years 2015-2020 (indices 0-4), map to x=0,1,2,3,4
    n = len(pre_shares)
    x_mean = (n - 1) / 2
    y_mean = sum(pre_shares) / n
    num = sum((i - x_mean) * (pre_shares[i] - y_mean) for i in range(n))
    den = sum((i - x_mean) ** 2 for i in range(n))
    pre_slope = num / den if den != 0 else 0
    pre_intercept = y_mean - pre_slope * x_mean
    # Pre trend at year 2019-20 is index 4, project forward
    # Years ahead from 2019-20: 2025=~5.5 yrs, 2030=~10.5 yrs, etc.
    # Each index = 1 school year

    # Post-pandemic trend: linear fit
    n2 = len(post_shares)
    x_mean2 = (n2 - 1) / 2
    y_mean2 = sum(post_shares) / n2
    num2 = sum((i - x_mean2) * (post_shares[i] - y_mean2) for i in range(n2))
    den2 = sum((i - x_mean2) ** 2 for i in range(n2))
    post_slope = num2 / den2 if den2 != 0 else 0
    post_intercept = y_mean2 - post_slope * x_mean2

    # Current TPS share (2023-24)
    current_share = hist["2023-24"]["tps"] / hist["2023-24"]["population"]

    print(f"  Pre-pandemic avg TPS share: {pre_avg:.4f} ({pre_avg*100:.1f}%)")
    print(f"  Pre-pandemic trend slope: {pre_slope:.6f}/yr")
    print(f"  Post-pandemic avg TPS share: {post_avg:.4f} ({post_avg*100:.1f}%)")
    print(f"  Post-pandemic trend slope: {post_slope:.6f}/yr")
    print(f"  Current TPS share (2023-24): {current_share:.4f} ({current_share*100:.1f}%)")

    # Compute multiplier for each scenario and year
    # Multiplier = scenario_tps_share / current_tps_share
    # This adjusts county enrollment proportionally
    multipliers = {}
    base_year = 2024  # our reference year

    for scenario in ["pre_hold", "pre_trend", "post_hold", "post_trend"]:
        multipliers[scenario] = {}
        for yr in YEARS:
            years_ahead = yr - base_year
            if scenario == "pre_hold":
                future_share = pre_avg
            elif scenario == "pre_trend":
                # Project pre-pandemic trend forward from 2019-20
                # 2019-20 is index 4 in pre series, yr is (yr - 2015) indices ahead
                idx = 4 + (yr - 2020)
                future_share = pre_intercept + pre_slope * idx
            elif scenario == "post_hold":
                future_share = post_avg
            elif scenario == "post_trend":
                # Project post-pandemic trend forward from 2023-24
                # 2023-24 is index 3 in post series
                idx = 3 + (yr - 2024)
                future_share = post_intercept + post_slope * idx

            # Floor at a reasonable minimum (don't go below 50% of current)
            future_share = max(future_share, current_share * 0.5)
            mult = future_share / current_share
            multipliers[scenario][yr] = round(mult, 4)

    print("  TPS share multipliers by scenario:")
    for scenario, yrs in multipliers.items():
        print(f"    {scenario}: {yrs}")

    return multipliers, current_share


# ---------------------------------------------------------------
# Step 5: Assemble county-level risk data
# ---------------------------------------------------------------
def assemble_risk_data(schools, hauer_data, census_data, tps_multipliers):
    """Combine all data sources into county-level school closure risk estimates."""
    print("\n[5/5] Assembling county-level risk data...")

    # Group schools by county
    county_schools = defaultdict(list)
    for s in schools:
        county_schools[s["fips"]].append(s)

    # Load ACS for county names
    acs_data = {}
    if ACS_CACHE.exists():
        with open(ACS_CACHE) as f:
            acs_data = json.load(f)

    # Get current school-age population baseline per county (from ACS)
    county_baseline_sa = {}
    for fips, acs in acs_data.items():
        county_baseline_sa[fips] = acs["school_age"]

    output = {}
    scenarios = ["pre_hold", "pre_trend", "post_hold", "post_trend"]
    demo_sources = ["hauer", "census"]

    for fips, school_list in county_schools.items():
        state_abbr = STATE_FIPS_TO_ABBR.get(fips[:2], fips[:2])
        county_name = acs_data.get(fips, {}).get("name", fips)

        total_enrollment = sum(s["enrollment"] for s in school_list)
        num_schools = len(school_list)

        entry = {
            "name": county_name,
            "schools_total": num_schools,
            "current_enrollment": total_enrollment,
            "scenarios": {}
        }

        baseline_sa = county_baseline_sa.get(fips, 0)

        for demo in demo_sources:
            for scenario in scenarios:
                scenario_key = f"{demo}_{scenario}"
                entry["scenarios"][scenario_key] = {}

                for yr in YEARS:
                    # Step 1: Demographic decline ratio
                    if demo == "hauer":
                        if fips in hauer_data and yr in hauer_data[fips]:
                            proj_sa = hauer_data[fips][yr]["school_age"]
                        elif fips in hauer_data:
                            # Interpolate
                            avail = sorted(hauer_data[fips].keys())
                            before = [y for y in avail if y <= yr]
                            after = [y for y in avail if y >= yr]
                            if before and after:
                                y1, y2 = before[-1], after[0]
                                if y1 == y2:
                                    proj_sa = hauer_data[fips][y1]["school_age"]
                                else:
                                    t = (yr - y1) / (y2 - y1)
                                    proj_sa = (hauer_data[fips][y1]["school_age"] +
                                               t * (hauer_data[fips][y2]["school_age"] -
                                                    hauer_data[fips][y1]["school_age"]))
                            else:
                                proj_sa = baseline_sa
                        else:
                            proj_sa = baseline_sa

                        # Use Hauer 2020 as baseline for ratio
                        if fips in hauer_data and 2020 in hauer_data[fips]:
                            base_sa = hauer_data[fips][2020]["school_age"]
                        else:
                            base_sa = baseline_sa
                    else:  # census
                        if census_data and fips in census_data:
                            proj = census_data[fips]["projections"]
                            proj_sa = proj.get(yr, baseline_sa)
                            base_sa = census_data[fips]["baseline_school_age"]
                        else:
                            proj_sa = baseline_sa
                            base_sa = baseline_sa

                    # Demographic ratio
                    demo_ratio = proj_sa / base_sa if base_sa > 0 else 1.0

                    # Step 2: TPS market share multiplier
                    tps_mult = tps_multipliers[scenario].get(yr, 1.0)

                    # Combined ratio
                    combined_ratio = demo_ratio * tps_mult

                    # Step 3: Apply to each school
                    projected_enrollment = 0
                    green = yellow = orange = red = below_threshold = 0

                    for s in school_list:
                        proj_enroll = s["enrollment"] * combined_ratio
                        projected_enrollment += proj_enroll
                        decline_pct = (1 - combined_ratio) * 100

                        if decline_pct < 10:
                            green += 1
                        elif decline_pct < 25:
                            yellow += 1
                        elif decline_pct < 50:
                            orange += 1
                        else:
                            red += 1

                        if proj_enroll < 100:
                            below_threshold += 1

                    pct_decline = (1 - projected_enrollment / total_enrollment) * 100 if total_enrollment > 0 else 0

                    entry["scenarios"][scenario_key][str(yr)] = {
                        "pct_decline": round(pct_decline, 1),
                        "schools_green": green,
                        "schools_yellow": yellow,
                        "schools_orange": orange,
                        "schools_red": red,
                        "schools_below_threshold": below_threshold,
                        "projected_enrollment": round(projected_enrollment),
                    }

        output[fips] = entry

    return output


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------
def main():
    print("=" * 60)
    print("  U.S. School Closure Risk Map — Data Processor")
    print("=" * 60)

    # Step 1: CCD schools
    print("\n[1/5] Loading CCD school data...")
    all_schools = fetch_ccd_schools()
    schools = filter_tps_schools(all_schools)
    print(f"  Filtered to {len(schools)} traditional public schools")

    # Step 2: Hauer
    hauer_data = process_hauer_school_age()

    # Step 3: Census
    census_data = process_census_school_age()

    # Step 4: TPS multipliers
    tps_multipliers, current_share = compute_tps_multipliers()

    # Step 5: Assemble
    output = assemble_risk_data(schools, hauer_data, census_data, tps_multipliers)

    # Write output
    out_path = OUTPUT_DIR / "school_risk_data.json"
    with open(out_path, "w") as f:
        json.dump(output, f)

    print(f"\n{'=' * 60}")
    print(f"  DONE! Generated school_risk_data.json")
    print(f"  Counties: {len(output)}")
    print(f"  File size: {out_path.stat().st_size / 1024:.0f} KB")
    print(f"{'=' * 60}")

    # Quick summary
    total_schools = sum(e["schools_total"] for e in output.values())
    total_enrollment = sum(e["current_enrollment"] for e in output.values())
    print(f"\n  Total TPS schools: {total_schools:,}")
    print(f"  Total enrollment: {total_enrollment:,}")

    # Sample scenario output
    sample_scenario = "hauer_post_trend"
    for yr_str in ["2030", "2050"]:
        below = sum(
            e["scenarios"].get(sample_scenario, {}).get(yr_str, {}).get("schools_below_threshold", 0)
            for e in output.values()
        )
        proj = sum(
            e["scenarios"].get(sample_scenario, {}).get(yr_str, {}).get("projected_enrollment", 0)
            for e in output.values()
        )
        decline = (1 - proj / total_enrollment) * 100 if total_enrollment > 0 else 0
        print(f"  {sample_scenario} {yr_str}: {decline:.1f}% decline, {below:,} schools below threshold")


if __name__ == "__main__":
    main()
