"""
Adintel + Pathmatics + MediaRadar Combiner - Streamlit App
Deploy to Streamlit Cloud for free: https://streamlit.io/cloud

Methodology v5:
- AdIntel: Keep ALL data (traditional + digital display/video + YouTube + Search) EXCEPT:
  - Streaming (covered by Pathmatics OTT)
  - Financial publishers: Twitch, Morningstar, Economist, Marketwatch, Investing.com,
    Investors.com, Zacks, TheAtlantic (covered by Pathmatics)
- Pathmatics: Add only Social Media and OTT/CTV (exclude Desktop/Mobile Display/Video and YouTube)
  - EXCEPTION: Keep Desktop/Mobile Display/Video for financial publishers listed above
- MediaRadar: Add only Podcast, Email, and Retail Media (Native) — US National market only
- YouTube: Labeled separately as 'YouTube (digital video)' from AdIntel distributor data

To run locally:
    pip install streamlit pandas openpyxl
    streamlit run streamlit_combiner.py
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime

st.set_page_config(page_title="Adintel + Pathmatics + MediaRadar Combiner", page_icon="📊", layout="wide")

# ========== CHANNELS TO EXCLUDE FROM PATHMATICS ==========
EXCLUDED_PATHMATICS_CHANNELS = {
    'Desktop Display', 'Desktop Video', 'Mobile Display', 'Mobile Video', 'YouTube'
}

# ========== PUBLISHERS TO EXCLUDE FROM ADINTEL (use Pathmatics instead) ==========
ADINTEL_EXCLUDE_DISTRIBUTORS = {
    'MORNINGSTAR', 'MORNINGSTAR.COM',
    'ECONOMIST', 'ECONOMIST.COM',
    'MARKETWATCH', 'MARKETWATCH.COM',
    'INVESTING.COM',
    'INVESTORS.COM',
    'ZACKS', 'ZACKS.COM',
    'THEATLANTIC', 'THEATLANTIC.COM', 'THE ATLANTIC',
    'TWITCH', 'TWITCH.TV',
}

# ========== PUBLISHERS TO KEEP FROM PATHMATICS ==========
PATHMATICS_KEEP_PUBLISHERS = {
    'twitch', 'twitch.tv',
    'morningstar.com',
    'economist.com',
    'marketwatch.com',
    'investing.com',
    'investors.com',
    'zacks.com',
    'theatlantic.com',
}

# ========== MEDIARADAR FORMATS TO INCLUDE ==========
INCLUDED_MEDIARADAR_FORMATS = {'Podcast', 'Native', 'Email'}

# ========== MEDIARADAR FORMAT REMAPPING ==========
MEDIARADAR_FORMAT_MAP = {
    'Podcast': {
        'Media Type': 'Podcast',
        'Media Category': 'Audio',
        'Middle Media Category': 'Audio',
    },
    'Native': {
        'Media Type': 'Retail Media',
        'Media Category': 'Digital',
        'Middle Media Category': 'Retail Media',
    },
    'Email': {
        'Media Type': 'Email',
        'Media Category': 'Digital',
        'Middle Media Category': 'Digital Email',
    },
}

# ========== OPTIONAL COLUMNS (cross-source) ==========
# (output_name, adintel_col, pathmatics_col, mediaradar_col)
OPTIONAL_COLUMNS = [
    ('Ad Buy Type',                 'Buy Type',             'Ad Buy Type',              None),
    ('Landing Page URL',            'Landing Page URL',     'Landing Page',             None),
    ('Ad Service Type',             'Ad Service Type',      'Purchase Channel Type',    None),
    ('Creative Type',               'Ad SubType',           'Creative Type',            None),
    ('First Seen',                  'First Appear Date',    'First Seen',               None),
    ('Creative Details',            'Creative Description', 'Text',                     None),
    ('Creative ID',                 'Creative ID',          'Creative Id',              None),
    ('Daypart',                     'Daypart',              None,                       None),
    ('Device (Adintel)',            'Device',               None,                       None),
    ('Delivery Platform (Adintel)', 'Delivery Platform',    None,                       None),
    ('Placement (Pathmatics)',      None,                   'Placement',                None),
    ('Program Name',                'Program Name',         None,                       None),
    ('Program Genre',               'Program Genre',        None,                       None),
]

# ========== ADINTEL-ONLY COLUMNS ==========
# No Pathmatics/MediaRadar equivalent — N/A for all other sources by design.
# Ad Size is handled separately due to custom Pathmatics concatenation logic.
ADINTEL_ONLY_COLUMNS = [
    'Clicks',
    'CPC', 'CPC (x.xx)',
    'CTR', 'CTR (x.xxx)',
    'Ad Visibility',
    'Advertiser Domain',
    'Advertiser Search Category',
    'Avg Rank',
    'Search Keyword',
    'Search Keyword Group',
    'Occurrence Indicator',
]


# ========== HELPER FUNCTIONS ==========

def detect_version(adintel_df):
    columns = [col.strip() for col in adintel_df.columns.tolist()]
    has_week = 'Week' in columns
    has_month = 'Month' in columns
    has_impressions = 'ImpE_P18_99' in columns or 'IMP_P2_99' in columns

    if has_week and has_impressions:
        return 'weekly_impressions', 'Weekly + Impressions'
    elif has_week:
        return 'weekly', 'Weekly'
    elif has_month and has_impressions:
        return 'monthly_impressions', 'Monthly + Impressions'
    elif has_month:
        return 'monthly', 'Monthly'
    return None, 'Unknown'


def detect_adintel_brand_col(adintel_df):
    if 'Brand Core' in adintel_df.columns:
        return 'Brand Core'
    elif 'Brand Variant' in adintel_df.columns:
        return 'Brand Variant'
    elif 'Brand' in adintel_df.columns:
        return 'Brand'
    return None


def detect_optional_columns(adintel_df, pathmatics_df, mr_df=None):
    detected = []
    for output_name, ai_col, path_col, mr_col in OPTIONAL_COLUMNS:
        found = False
        if ai_col and ai_col in adintel_df.columns:
            found = True
        if path_col and path_col in pathmatics_df.columns:
            found = True
        if mr_col and mr_df is not None and mr_col in mr_df.columns:
            found = True
        if found:
            detected.append(output_name)
    return detected


def detect_ad_size(adintel_df, pathmatics_df):
    """Ad Size is optional but requires custom logic: concat Width*Height from Pathmatics."""
    has_adintel = 'Ad Size' in adintel_df.columns
    has_pathmatics = 'Width' in pathmatics_df.columns and 'Height' in pathmatics_df.columns
    return has_adintel or has_pathmatics


def detect_adintel_only_columns(adintel_df):
    """Detect which AdIntel-only columns are present in the upload."""
    return [col for col in ADINTEL_ONLY_COLUMNS if col in adintel_df.columns]


def check_column_warnings(adintel_df, pathmatics_df):
    """
    Return warning messages when a column exists in one source but not the other.
    Shown before processing so users can re-export if needed.
    """
    warnings = []

    for output_name, ai_col, path_col, _ in OPTIONAL_COLUMNS:
        ai_has = ai_col is not None and ai_col in adintel_df.columns
        path_has = path_col is not None and path_col in pathmatics_df.columns

        if ai_has and path_col is not None and not path_has:
            warnings.append(
                f"**{output_name}**: AdIntel has `{ai_col}` but your Pathmatics export "
                f"is missing `{path_col}` — Pathmatics rows will show N/A."
            )
        elif path_has and ai_col is not None and not ai_has:
            warnings.append(
                f"**{output_name}**: Pathmatics has `{path_col}` but your AdIntel export "
                f"is missing `{ai_col}` — AdIntel rows will show N/A."
            )

    # Ad Size — separate check (two-column Pathmatics logic)
    ai_has_size = 'Ad Size' in adintel_df.columns
    path_has_size = 'Width' in pathmatics_df.columns and 'Height' in pathmatics_df.columns
    if ai_has_size and not path_has_size:
        warnings.append(
            "**Ad Size**: AdIntel has `Ad Size` but your Pathmatics export is missing "
            "`Width` and/or `Height` — Pathmatics rows will show N/A."
        )
    elif path_has_size and not ai_has_size:
        warnings.append(
            "**Ad Size**: Pathmatics has `Width`/`Height` but your AdIntel export is "
            "missing `Ad Size` — AdIntel rows will show N/A."
        )

    return warnings


def map_optional_columns(df, source, detected_optionals):
    for output_name, ai_col, path_col, mr_col in OPTIONAL_COLUMNS:
        if output_name not in detected_optionals:
            continue
        if source == 'AdIntel' and ai_col and ai_col in df.columns:
            df[output_name] = df[ai_col]
        elif source == 'Pathmatics' and path_col and path_col in df.columns:
            df[output_name] = df[path_col]
        elif source == 'MediaRadar' and mr_col and mr_col in df.columns:
            df[output_name] = df[mr_col]
        else:
            df[output_name] = 'N/A'
    return df


def assign_pathmatics_middle_category(channel):
    ch = str(channel).strip()
    social_platforms = {'Facebook', 'Instagram', 'Snapchat', 'TikTok',
                        'X', 'Twitter', 'LinkedIn', 'Pinterest', 'Reddit'}
    if ch == 'OTT':
        return 'OTT'
    elif ch == 'YouTube':
        return 'Digital Video'
    elif ch in social_platforms:
        return 'Digital Social'
    elif ch in ('Desktop Display', 'Mobile Display'):
        return 'Digital Display'
    elif ch in ('Desktop Video', 'Mobile Video'):
        return 'Digital Video'
    return 'Digital'


def assign_adintel_middle_category(row):
    media_type = str(row.get('Media Type', '')).strip().lower()
    if 'youtube' in media_type:
        return 'Digital Video'
    elif 'digital' in media_type and 'video' in media_type:
        return 'Digital Video'
    elif 'digital' in media_type and 'display' in media_type:
        return 'Digital Display'
    elif 'digital' in media_type and 'search' in media_type:
        return 'Digital Search'
    elif 'radio' in media_type:
        return 'Audio'
    return row.get('Media Category', 'N/A')


def group_media_type(media_type):
    mt = str(media_type).strip()
    social = {'Facebook', 'Instagram', 'Snapchat', 'TikTok',
              'X', 'Twitter', 'LinkedIn', 'Pinterest', 'Reddit'}
    spanish_tv = {'Spanish Language Cable TV', 'Spanish Language Network TV'}
    clearance_tv = {'Network Clearance Spot TV', 'Syndicated Clearance Spot TV'}
    digital_video = {'National Digital-Video', 'Local Digital-Video', 'Desktop Video', 'Mobile Video'}
    digital_display = {'National Digital-Display', 'Local Digital-Display', 'Desktop Display', 'Mobile Display'}
    digital_search = {'National Digital-Search', 'Local Digital-Search'}
    audio = set()

    if mt in social:
        return 'Social Media'
    elif mt in spanish_tv:
        return 'Spanish Language TV'
    elif mt in clearance_tv:
        return 'Network/Syndicated Clearance Spot TV'
    elif mt in digital_video:
        return 'Digital Video'
    elif mt in digital_display:
        return 'Digital Display'
    elif mt in digital_search:
        return 'Digital Search'
    elif mt in audio:
        return 'Audio'
    elif mt == 'Email':
        return 'Digital Email'
    elif mt == 'Retail Media':
        return 'Retail Media'
    return mt


def read_adintel(uploaded_file):
    filename = uploaded_file.name
    if filename.endswith('.csv'):
        df = pd.read_csv(uploaded_file, skiprows=2)
    else:
        df = pd.read_excel(uploaded_file, sheet_name='Report', skiprows=3)
    df.columns = df.columns.str.strip()
    return df


def read_pathmatics(uploaded_file):
    filename = uploaded_file.name
    if filename.endswith('.csv'):
        df = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df = pd.read_excel(uploaded_file, skiprows=1)
    df.columns = df.columns.str.strip()
    return df


def read_mediaradar(uploaded_file):
    filename = uploaded_file.name
    if filename.endswith('.csv'):
        raw = pd.read_csv(uploaded_file, header=None)
    else:
        raw = pd.read_excel(uploaded_file, sheet_name='Report Builder', header=None)

    header_row = None
    for idx, row in raw.iterrows():
        if row.astype(str).str.strip().str.lower().eq('format').any():
            header_row = idx
            break

    if header_row is None:
        raise ValueError("Could not find 'Format' column in MediaRadar file. Check the file structure.")

    uploaded_file.seek(0)
    if filename.endswith('.csv'):
        df = pd.read_csv(uploaded_file, skiprows=header_row)
    else:
        df = pd.read_excel(uploaded_file, sheet_name='Report Builder', skiprows=header_row)

    df.columns = df.columns.str.strip()
    return df


def process_mediaradar(mr_df, date_col, detected_optionals, include_ad_size, adintel_only_detected):
    mr_df['Format'] = mr_df['Format'].str.strip()
    before = len(mr_df)
    mr_df = mr_df[mr_df['Format'].isin(INCLUDED_MEDIARADAR_FORMATS)]
    formats_excluded = before - len(mr_df)

    if mr_df.empty:
        return pd.DataFrame(), 0, formats_excluded, 0

    market_excluded = 0
    if 'Market' in mr_df.columns:
        before = len(mr_df)
        mr_df = mr_df[mr_df['Market'].str.strip() == 'US National']
        market_excluded = before - len(mr_df)
        mr_df['Market'] = 'National'

    if mr_df.empty:
        return pd.DataFrame(), 0, formats_excluded, market_excluded

    non_month_cols = ['Parent', 'Product Line', 'Format', 'Detailed Property',
                      'Media Property', 'National/Local', 'Market']
    month_cols = [c for c in mr_df.columns if c not in non_month_cols]

    mr_melted = mr_df.melt(
        id_vars=[c for c in non_month_cols if c in mr_df.columns],
        value_vars=month_cols,
        var_name='Month_Raw',
        value_name='Dollars'
    )

    mr_melted['Dollars'] = pd.to_numeric(mr_melted['Dollars'].astype(str).str.replace(
        r'[\$,]', '', regex=True), errors='coerce')
    mr_melted = mr_melted.dropna(subset=['Dollars'])
    mr_melted = mr_melted[mr_melted['Dollars'] != 0]

    if mr_melted.empty:
        return pd.DataFrame(), 0, formats_excluded, market_excluded

    mr_melted[date_col] = pd.to_datetime(mr_melted['Month_Raw'], format='%b %Y', errors='coerce')
    if mr_melted[date_col].isna().all():
        mr_melted[date_col] = pd.to_datetime(mr_melted['Month_Raw'], errors='coerce')

    mr_melted['Media Type'] = mr_melted['Format'].map(
        lambda x: MEDIARADAR_FORMAT_MAP.get(x, {}).get('Media Type', x))
    mr_melted['Media Category'] = mr_melted['Format'].map(
        lambda x: MEDIARADAR_FORMAT_MAP.get(x, {}).get('Media Category', 'N/A'))
    mr_melted['Middle Media Category'] = mr_melted['Format'].map(
        lambda x: MEDIARADAR_FORMAT_MAP.get(x, {}).get('Middle Media Category', 'N/A'))

    mr_melted['Source'] = 'MediaRadar'
    mr_melted['Subsidiary'] = mr_melted.get('Parent', 'N/A')
    mr_melted['Brand Variant'] = mr_melted.get('Product Line', 'N/A')
    mr_melted['Distributor'] = mr_melted.get('Detailed Property', 'N/A')
    mr_melted['Distributor Description'] = mr_melted.get('Media Property', 'N/A')
    mr_melted['Market'] = mr_melted.get('Market', 'N/A')
    mr_melted['Commercial Duration'] = 'N/A'
    mr_melted['Estimated Impressions'] = 0
    mr_melted['Parent'] = mr_melted.get('Parent', 'N/A')

    mr_melted = map_optional_columns(mr_melted, 'MediaRadar', detected_optionals)

    if include_ad_size:
        mr_melted['Ad Size'] = 'N/A'

    for col in adintel_only_detected:
        mr_melted[col] = 'N/A'

    return mr_melted, len(mr_melted), formats_excluded, market_excluded


def process_files(adintel_df, pathmatics_df, version, mr_df=None):
    is_weekly = 'weekly' in version
    include_impressions = 'impressions' in version
    date_col = 'Date' if is_weekly else 'Month'

    # ========== DETECT COLUMNS ==========
    detected_optionals = detect_optional_columns(adintel_df, pathmatics_df, mr_df)
    include_ad_size = detect_ad_size(adintel_df, pathmatics_df)
    adintel_only_detected = detect_adintel_only_columns(adintel_df)

    # ========== ADINTEL DATES ==========
    footer_removed = 0
    if is_weekly:
        adintel_df['Date'] = adintel_df['Week'].str.split(' - ').str[0]
        adintel_df['Date'] = pd.to_datetime(adintel_df['Date'], format='%m/%d/%Y', errors='coerce')
        before_clean = len(adintel_df)
        adintel_df = adintel_df[adintel_df['Date'].notna()]
        footer_removed = before_clean - len(adintel_df)
    else:
        adintel_df['Month'] = pd.to_datetime(adintel_df['Month'], errors='coerce')
        before_clean = len(adintel_df)
        adintel_df = adintel_df[adintel_df['Month'].notna()]
        footer_removed = before_clean - len(adintel_df)
        adintel_df['Month'] = adintel_df['Month'].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)

    # Filter out Streaming
    streaming_removed = 0
    if 'Media Category' in adintel_df.columns:
        before = len(adintel_df)
        adintel_df = adintel_df[adintel_df['Media Category'].str.strip().str.lower() != 'streaming']
        streaming_removed = before - len(adintel_df)

    # Label YouTube distributors separately
    youtube_count = 0
    if 'Distributor' in adintel_df.columns:
        youtube_mask = adintel_df['Distributor'].str.strip().str.lower().str.contains('youtube', na=False)
        adintel_df.loc[youtube_mask, 'Media Type'] = 'YouTube (digital video)'
        youtube_count = youtube_mask.sum()

    # Filter out financial publishers from AdIntel
    financial_removed = 0
    if 'Distributor' in adintel_df.columns:
        before = len(adintel_df)
        exclude_mask = adintel_df['Distributor'].str.strip().str.upper().isin(ADINTEL_EXCLUDE_DISTRIBUTORS)
        adintel_df = adintel_df[~exclude_mask]
        financial_removed = before - len(adintel_df)

    # Detect brand column
    adintel_brand_col = detect_adintel_brand_col(adintel_df)
    if adintel_brand_col:
        adintel_df['Brand Variant'] = adintel_df[adintel_brand_col]

    has_adintel_parent = 'Parent' in adintel_df.columns

    # Map optional columns for AdIntel
    adintel_df = map_optional_columns(adintel_df, 'AdIntel', detected_optionals)

    # Ad Size — AdIntel (direct passthrough; column already present if detected)
    if include_ad_size and 'Ad Size' not in adintel_df.columns:
        adintel_df['Ad Size'] = 'N/A'

    # ========== PATHMATICS ==========
    before_filter = len(pathmatics_df)
    kept_publishers = 0

    if 'Publisher' in pathmatics_df.columns:
        keep_publisher_mask = pathmatics_df['Publisher'].str.lower().str.strip().isin(PATHMATICS_KEEP_PUBLISHERS)
        channel_exclude_mask = pathmatics_df['Channel'].str.strip().isin(EXCLUDED_PATHMATICS_CHANNELS)
        pathmatics_df = pathmatics_df[~channel_exclude_mask | keep_publisher_mask]
        kept_publishers = keep_publisher_mask.sum()
    else:
        pathmatics_df = pathmatics_df[~pathmatics_df['Channel'].str.strip().isin(EXCLUDED_PATHMATICS_CHANNELS)]

    channels_removed = before_filter - len(pathmatics_df)

    if 'Brand Leaf' in pathmatics_df.columns:
        pathmatics_df['Brand Variant'] = pathmatics_df['Brand Leaf']
    elif 'Brand Root' in pathmatics_df.columns:
        pathmatics_df['Brand Variant'] = pathmatics_df['Brand Root']

    if 'Brand Root' in pathmatics_df.columns:
        pathmatics_df['Subsidiary'] = pathmatics_df['Brand Root']

    if 'Advertiser' in pathmatics_df.columns:
        pathmatics_df['Parent'] = pathmatics_df['Advertiser']

    # Map optional columns for Pathmatics
    pathmatics_df = map_optional_columns(pathmatics_df, 'Pathmatics', detected_optionals)

    # Ad Size — Pathmatics (concatenate Width*Height)
    if include_ad_size:
        if 'Width' in pathmatics_df.columns and 'Height' in pathmatics_df.columns:
            pathmatics_df['Ad Size'] = (
                pathmatics_df['Width'].astype(str).str.strip() + '*' +
                pathmatics_df['Height'].astype(str).str.strip()
            )
        else:
            pathmatics_df['Ad Size'] = 'N/A'

    # AdIntel-only columns: fill N/A for Pathmatics
    for col in adintel_only_detected:
        pathmatics_df[col] = 'N/A'

    pathmatics_df['Date'] = pd.to_datetime(pathmatics_df['Date'], errors='coerce')

    if not is_weekly:
        pathmatics_df['Date'] = pathmatics_df['Date'] + pd.Timedelta(days=6)
        pathmatics_df['Date'] = pd.to_datetime(pathmatics_df['Date'].dt.strftime('%B %Y'), format='%B %Y')

    pathmatics_df['Media Category'] = 'Digital'
    pathmatics_df['Market'] = 'NATIONAL'
    pathmatics_df['Source'] = 'Pathmatics'
    pathmatics_df['Distributor Description'] = 'N/A'
    pathmatics_df['Duration'] = pathmatics_df['Duration'].fillna('N/A')

    if 'Impressions' not in pathmatics_df.columns:
        pathmatics_df['Impressions'] = 0

    pathmatics_df['Middle Media Category'] = pathmatics_df['Channel'].apply(assign_pathmatics_middle_category)

    pathmatics_df.rename(columns={
        'Channel': 'Media Type',
        'Duration': 'Commercial Duration',
        'Publisher': 'Distributor',
        'Spend (USD)': 'Dollars',
        'Impressions': 'Estimated Impressions',
        'Date': date_col,
    }, inplace=True)

    # ========== ADINTEL FOR COMBINATION ==========
    adintel_df['Source'] = 'AdIntel'
    adintel_df['Middle Media Category'] = adintel_df.apply(assign_adintel_middle_category, axis=1)

    rename_map = {}
    if include_impressions:
        if 'ImpE_P18_99' in adintel_df.columns:
            rename_map['ImpE_P18_99'] = 'Estimated Impressions'
        elif 'IMP_P2_99' in adintel_df.columns:
            rename_map['IMP_P2_99'] = 'Estimated Impressions'
        else:
            adintel_df['Estimated Impressions'] = 0

    if rename_map:
        adintel_df.rename(columns=rename_map, inplace=True)

    # ========== DETERMINE IF PARENT COLUMN NEEDED ==========
    include_parent = has_adintel_parent or (mr_df is not None)

    # ========== BUILD COLUMN LIST ==========
    base_columns = [
        'Source', 'Subsidiary', 'Brand Variant', 'Distributor',
        'Distributor Description', 'Media Type', 'Media Category',
        'Middle Media Category', 'Market', 'Commercial Duration',
    ]
    if include_parent:
        base_columns.insert(1, 'Parent')

    for opt_col in detected_optionals:
        base_columns.append(opt_col)

    if include_ad_size:
        base_columns.append('Ad Size')

    for col in adintel_only_detected:
        base_columns.append(col)

    base_columns.append(date_col)
    base_columns.append('Dollars')

    if include_impressions:
        base_columns.append('Estimated Impressions')

    # ========== ENSURE COLUMNS EXIST ==========
    for df in [adintel_df, pathmatics_df]:
        if 'Brand Variant' not in df.columns:
            df['Brand Variant'] = 'N/A'
        if include_parent and 'Parent' not in df.columns:
            df['Parent'] = 'N/A'
        if 'Estimated Impressions' not in df.columns:
            df['Estimated Impressions'] = 0
        for opt_col in detected_optionals:
            if opt_col not in df.columns:
                df[opt_col] = 'N/A'
        if include_ad_size and 'Ad Size' not in df.columns:
            df['Ad Size'] = 'N/A'
        for col in adintel_only_detected:
            if col not in df.columns:
                df[col] = 'N/A'

    pathmatics_selected = pathmatics_df[base_columns]
    adintel_selected = adintel_df[base_columns]
    adintel_selected = adintel_selected.rename(columns={'Dollars ': 'Dollars'})

    frames = [pathmatics_selected, adintel_selected]

    # ========== PROCESS MEDIARADAR IF PROVIDED ==========
    mr_count = 0
    mr_formats_excluded = 0
    mr_market_excluded = 0
    if mr_df is not None:
        mr_processed, mr_count, mr_formats_excluded, mr_market_excluded = process_mediaradar(
            mr_df, date_col, detected_optionals, include_ad_size, adintel_only_detected
        )
        if not mr_processed.empty:
            for col in base_columns:
                if col not in mr_processed.columns:
                    mr_processed[col] = 'N/A'
            frames.append(mr_processed[base_columns])

    combined_df = pd.concat(frames, ignore_index=True)

    NUMERIC_COLS = {'Dollars', 'Estimated Impressions', 'Clicks', 'CPC', 'CPC (x.xx)', 'CTR', 'CTR (x.xxx)', 'Avg Rank'}
    for col in combined_df.columns:
        if col != date_col and col not in NUMERIC_COLS:
            combined_df[col] = combined_df[col].fillna('N/A')

    combined_df['Media Type Grouped'] = combined_df['Media Type'].apply(group_media_type)

    return (
        combined_df, len(pathmatics_selected), len(adintel_selected),
        streaming_removed, channels_removed, youtube_count,
        mr_count, mr_formats_excluded, detected_optionals, financial_removed,
        kept_publishers, footer_removed, mr_market_excluded,
        include_ad_size, adintel_only_detected
    )


# ========== STREAMLIT UI ==========

st.title("📊 Adintel + Pathmatics + MediaRadar Combiner")
st.markdown("""
Upload your files to automatically combine them.

**Methodology v5:**
- **AdIntel** → All traditional media + digital display/video + YouTube + Search
  - *Excludes: Streaming, Twitch, Morningstar, Economist, Marketwatch, Investing.com, Investors.com, Zacks, TheAtlantic*
- **Pathmatics** → Social media (FB, IG, TikTok, etc.) + OTT/CTV
  - *Includes: Desktop/Mobile Display/Video for financial publishers (Twitch, Morningstar, etc.)*
- **MediaRadar** *(optional)* → Podcast, Email, Retail Media (Native) — US National only
- No overlap between sources
""")

with st.expander("📖 Column Requirements by Source"):
    st.markdown("""
    ### Mandatory Columns

    | Output Column | AdIntel | Pathmatics | MediaRadar |
    |---|---|---|---|
    | Subsidiary | `Subsidiary` | `Brand Root` | `Parent` |
    | Brand Variant | `Brand Core` or `Brand Variant` | `Brand Leaf` | `Product Line` |
    | Distributor | `Distributor` | `Publisher` | `Detailed Property` |
    | Distributor Description | `Distributor Description` | — (N/A) | `Media Property` |
    | Media Type | `Media Type` | `Channel` | `Format` |
    | Media Category | `Media Category` | — (auto: Digital) | — (auto-mapped) |
    | Market | `Market` | — (auto: NATIONAL) | `Market` (US National only) |
    | Commercial Duration | `Commercial Duration` | `Duration` | — (N/A) |
    | Date | `Month` or `Week` | `Date` | Month columns (e.g., `Jan 2025`) |
    | Dollars | `Dollars` | `Spend (USD)` | Month columns (unpivoted) |

    ### Conditional Columns (appear when detected)

    | Output Column | AdIntel | Pathmatics | MediaRadar |
    |---|---|---|---|
    | Parent | `Parent` | `Advertiser` | `Parent` |
    | Estimated Impressions | `ImpE_P18_99` or `IMP_P2_99` | `Impressions` | — (N/A) |

    ### Optional Columns — Cross-Source (appear when detected in any source; N/A for sources missing the column)

    | Output Column | AdIntel | Pathmatics | MediaRadar |
    |---|---|---|---|
    | Ad Buy Type | `Buy Type` | `Ad Buy Type` | — |
    | Landing Page URL | `Landing Page URL` | `Landing Page` | — |
    | Ad Service Type | `Ad Service Type` | `Purchase Channel Type` | — |
    | Creative Type | `Ad SubType` | `Creative Type` | — |
    | First Seen | `First Appear Date` | `First Seen` | — |
    | Creative Details | `Creative Description` | `Text` | — |
    | Creative ID | `Creative ID` | `Creative Id` | — |
    | Ad Size | `Ad Size` | `Width` + `Height` (auto-concatenated as `W*H`) | — |
    | Daypart | `Daypart` | — (N/A) | — (N/A) |
    | Device (Adintel) | `Device` | — | — |
    | Delivery Platform (Adintel) | `Delivery Platform` | — | — |
    | Placement (Pathmatics) | — | `Placement` | — |
    | Program Name | `Program Name` | — | — |
    | Program Genre | `Program Genre` | — | — |

    ### Optional Columns — AdIntel Only (appear when detected; N/A for all other sources by design)

    | Output Column | AdIntel Source Column |
    |---|---|
    | Clicks | `Clicks` |
    | CPC | `CPC` |
    | CTR | `CTR` |
    | Ad Visibility | `Ad Visibility` |
    | Advertiser Domain | `Advertiser Domain` |
    | Advertiser Search Category | `Advertiser Search Category` |
    | Avg Rank | `Avg Rank` |
    | Search Keyword | `Search Keyword` |
    | Search Keyword Group | `Search Keyword Group` |

    ### Auto-Generated Columns

    | Column | Description |
    |---|---|
    | Source | `AdIntel`, `Pathmatics`, or `MediaRadar` |
    | Middle Media Category | Mid-level grouping (Digital Video, Digital Display, Digital Search, Digital Social, OTT, Audio, Retail Media, etc.) |
    | Media Type Grouped | Top-level grouping (Digital Video, Digital Display, Digital Search, Social Media, Audio, Retail Media, etc.) |

    *MediaRadar is optional. Columns marked "—" are filled as N/A.*
    """)

with st.expander("📋 Publisher Handling Rules"):
    st.markdown("""
    ### AdIntel Excluded Publishers (use Pathmatics instead)
    - `TWITCH`, `TWITCH.TV`
    - `MORNINGSTAR`, `MORNINGSTAR.COM`
    - `ECONOMIST`, `ECONOMIST.COM`
    - `MARKETWATCH`, `MARKETWATCH.COM`
    - `INVESTING.COM`, `INVESTORS.COM`
    - `ZACKS`, `ZACKS.COM`
    - `THEATLANTIC`, `THEATLANTIC.COM`, `THE ATLANTIC`

    ### Pathmatics Keep Publishers (even when excluding Desktop/Mobile Display/Video)
    - `twitch`, `twitch.tv`, `morningstar.com`, `economist.com`, `marketwatch.com`
    - `investing.com`, `investors.com`, `zacks.com`, `theatlantic.com`

    *This ensures financial and gaming publishers have accurate coverage from the source with better tracking.*
    """)

# ========== FILE UPLOADERS ==========
col1, col2, col3 = st.columns(3)

with col1:
    st.subheader("Adintel File")
    adintel_file = st.file_uploader("Upload Adintel file", type=['csv', 'xlsx'], key='adintel')

with col2:
    st.subheader("Pathmatics File")
    pathmatics_file = st.file_uploader("Upload Pathmatics file", type=['csv', 'xlsx'], key='pathmatics')

with col3:
    st.subheader("MediaRadar File (Optional)")
    mediaradar_file = st.file_uploader("Upload MediaRadar file", type=['csv', 'xlsx'], key='mediaradar')

if adintel_file and pathmatics_file:
    with st.spinner("Reading files..."):
        try:
            adintel_df = read_adintel(adintel_file)
            pathmatics_df = read_pathmatics(pathmatics_file)
            version, version_display = detect_version(adintel_df)

            mr_df = None
            if mediaradar_file:
                mr_df = read_mediaradar(mediaradar_file)

            if version is None:
                st.error("Could not detect file format. Make sure Adintel file has 'Week' or 'Month' column.")
            else:
                brand_col = detect_adintel_brand_col(adintel_df)
                has_parent = 'Parent' in adintel_df.columns
                opt_cols = detect_optional_columns(adintel_df, pathmatics_df, mr_df)
                has_ad_size = detect_ad_size(adintel_df, pathmatics_df)
                ai_only_cols = detect_adintel_only_columns(adintel_df)

                status = f"✅ Detected format: **{version_display}** | Brand column: **{brand_col or 'Not found'}**"
                if has_parent:
                    status += " | Parent: **detected**"
                all_opt = opt_cols + (['Ad Size'] if has_ad_size else []) + ai_only_cols
                if all_opt:
                    status += f" | Optional columns: **{', '.join(all_opt)}**"
                st.success(status)

                # ========== COLUMN MISMATCH WARNINGS ==========
                col_warnings = check_column_warnings(adintel_df, pathmatics_df)
                if col_warnings:
                    st.warning(
                        "⚠️ **Column mismatch detected** — the following columns are present in one "
                        "source but not the other. Rows from the missing source will show N/A. "
                        "You may want to re-export before continuing."
                    )
                    for w in col_warnings:
                        st.markdown(f"- {w}")

                cols = st.columns(3 if mr_df is not None else 2)
                with cols[0]:
                    st.metric("Adintel Rows", f"{len(adintel_df):,}")
                with cols[1]:
                    st.metric("Pathmatics Rows", f"{len(pathmatics_df):,}")
                if mr_df is not None:
                    with cols[2]:
                        st.metric("MediaRadar Rows", f"{len(mr_df):,}")

                if st.button("🚀 Process & Combine", type="primary"):
                    with st.spinner("Processing..."):
                        start_time = datetime.now()
                        (
                            combined_df, path_count, adin_count,
                            streaming_rm, channels_rm, yt_count,
                            mr_count, mr_fmt_excl, detected_opts, financial_rm,
                            kept_pubs, footer_rm, mr_market_excl,
                            incl_ad_size, ai_only_detected
                        ) = process_files(
                            adintel_df.copy(), pathmatics_df.copy(), version,
                            mr_df=mr_df.copy() if mr_df is not None else None
                        )
                        elapsed = (datetime.now() - start_time).total_seconds()

                    st.success(f"✅ Processing complete in {elapsed:.1f} seconds!")

                    result_cols = st.columns(4 if mr_df is not None else 3)
                    with result_cols[0]:
                        st.metric("Pathmatics (Social + OTT + Financial)", f"{path_count:,}")
                    with result_cols[1]:
                        st.metric("AdIntel (All media)", f"{adin_count:,}")
                    if mr_df is not None:
                        with result_cols[2]:
                            st.metric("MediaRadar (Podcast/Email/Retail)", f"{mr_count:,}")
                        with result_cols[3]:
                            st.metric("Combined Total", f"{len(combined_df):,}")
                    else:
                        with result_cols[2]:
                            st.metric("Combined Total", f"{len(combined_df):,}")

                    with st.expander("📋 Processing Details"):
                        st.write(f"**AdIntel footer/metadata rows removed:** {footer_rm:,}")
                        st.write(f"**AdIntel Streaming rows removed:** {streaming_rm:,} (covered by Pathmatics OTT)")
                        st.write(f"**AdIntel financial publisher rows removed:** {financial_rm:,} (Twitch, Morningstar, etc.)")
                        st.write(f"**AdIntel YouTube rows relabeled:** {yt_count:,} → 'YouTube (digital video)'")
                        st.write(f"**Pathmatics rows excluded:** {channels_rm:,} (Desktop/Mobile Display/Video + YouTube)")
                        st.write(f"**Pathmatics financial publisher rows kept:** {kept_pubs:,} (Twitch, Morningstar, etc.)")
                        st.write("**Pathmatics channels kept:** Social platforms + OTT/CTV + financial publishers")
                        if mr_df is not None:
                            st.write(f"**MediaRadar rows included:** {mr_count:,} (Podcast + Email + Retail Media)")
                            st.write(f"**MediaRadar format rows excluded:** {mr_fmt_excl:,} (formats already covered by AdIntel/Pathmatics)")
                            st.write(f"**MediaRadar market rows excluded:** {mr_market_excl:,} (non-US National)")
                        if detected_opts:
                            st.write(f"**Optional cross-source columns included:** {', '.join(detected_opts)}")
                        if incl_ad_size:
                            st.write("**Ad Size:** included (AdIntel direct / Pathmatics Width×Height concatenated)")
                        if ai_only_detected:
                            st.write(f"**AdIntel-only columns included:** {', '.join(ai_only_detected)}")

                    with st.expander("📊 Media Type Breakdown"):
                        breakdown_df = combined_df[['Source', 'Media Type Grouped']].copy()
                        breakdown_df['Dollars'] = pd.to_numeric(combined_df['Dollars'], errors='coerce').fillna(0)
                        source_summary = breakdown_df.groupby(['Source', 'Media Type Grouped'])['Dollars'].sum().reset_index()
                        source_summary['Dollars'] = source_summary['Dollars'].apply(lambda x: f"${x:,.0f}")
                        st.dataframe(source_summary, use_container_width=True)

                    st.subheader("Preview (first 100 rows)")
                    st.dataframe(combined_df.head(100), use_container_width=True)

                    csv_buffer = io.StringIO()
                    combined_df.to_csv(csv_buffer, index=False)
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"Combined_{version_display.replace(' + ', '_').replace(' ', '')}_{timestamp}.csv"

                    st.download_button(
                        label="📥 Download Combined CSV",
                        data=csv_buffer.getvalue(),
                        file_name=filename,
                        mime="text/csv",
                        type="primary"
                    )

        except Exception as e:
            st.error(f"❌ Error processing files: {str(e)}")
            st.exception(e)
else:
    st.info("👆 Please upload Adintel and Pathmatics files to begin. MediaRadar is optional.")

st.markdown("---")
st.markdown("""
*Methodology v5 — Auto-detects Weekly/Monthly, Impressions, Brand column, Parent column, optional cross-source columns, Ad Size, and AdIntel-only Search columns*

| Source | Covers | Excludes |
|--------|--------|----------|
| **AdIntel** | TV, Radio, Print, Outdoor, Digital Display, Digital Video, YouTube, Search | Streaming, Twitch, Morningstar, Economist, Marketwatch, Investing.com, Investors.com, Zacks, TheAtlantic |
| **Pathmatics** | Social Media (FB, IG, TikTok, Snap, X, LinkedIn, Pinterest, Reddit) + OTT/CTV + Financial publishers | Desktop/Mobile Display/Video (except financial publishers) |
| **MediaRadar** | Podcast → Audio, Email → Digital, Retail Media → Digital (Retail Media) | Non-US National markets |
""")
