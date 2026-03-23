"""
Adintel + Pathmatics + MediaRadar Combiner - Streamlit App
Deploy to Streamlit Cloud for free: https://streamlit.io/cloud

Methodology v4:
- AdIntel: Keep ALL data (traditional + digital display/video + YouTube) EXCEPT Twitch.tv (covered by Pathmatics)
- Pathmatics: Add only Social Media and OTT/CTV (exclude Desktop/Mobile Display/Video and YouTube)
- MediaRadar: Add only Podcast, Email, and Retail Media (Native) — exclude all other formats
- AdIntel Streaming: Excluded (Pathmatics OTT has broader coverage)
- AdIntel Twitch.tv: Excluded (Pathmatics Publisher data used instead)
- YouTube: Labeled separately as 'YouTube (Digital Video)' from AdIntel distributor data

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
        'Media Category': 'Retail Media',
        'Middle Media Category': 'Retail Media',
    },
    'Email': {
        'Media Type': 'Email',
        'Media Category': 'Digital',
        'Middle Media Category': 'Digital Email',
    },
}

# ========== OPTIONAL COLUMN DETECTION ==========
# (output_name, adintel_source, pathmatics_source, mediaradar_source)
# Daypart is now optional — only included when detected in AdIntel
OPTIONAL_COLUMNS = [
    ('Landing Page', 'Landing Page URL', 'Landing Page', None),
    ('Buy Type', 'Buy Type', 'Ad Buy Type', None),
    ('Daypart', 'Daypart', None, None),
    ('Device (Adintel)', 'Device', None, None),
    ('Delivery Platform (Adintel)', 'Delivery Platform', None, None),
    ('Placement (Pathmatics)', None, 'Placement', None),
    ('Program Name', 'Program Name', None, None),   # ← ADD
    ('Program Genre', 'Program Genre', None, None),  # ← ADD
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
    """Detect which brand column AdIntel uses (varies by export)."""
    if 'Brand Core' in adintel_df.columns:
        return 'Brand Core'
    elif 'Brand Variant' in adintel_df.columns:
        return 'Brand Variant'
    elif 'Brand' in adintel_df.columns:
        return 'Brand'
    return None


def detect_optional_columns(adintel_df, pathmatics_df, mr_df=None):
    """Detect which optional columns are available across sources."""
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


def map_optional_columns(df, source, detected_optionals):
    """Map source-specific optional columns to standardized output names."""
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
    elif 'radio' in media_type:
        return 'Audio'
    return row.get('Media Category', 'N/A')


def group_media_type(media_type):
    mt = str(media_type).strip()
    social = {'Facebook', 'Instagram', 'Snapchat', 'TikTok',
              'X', 'Twitter', 'LinkedIn', 'Pinterest', 'Reddit'}
    spanish_tv = {'Spanish Language Cable TV', 'Spanish Language Network TV'}
    clearance_tv = {'Network Clearance Spot TV', 'Syndicated Clearance Spot TV'}
    digital_video = {'National Digital-Video', 'Local Digital-Video', 'YouTube (Digital Video)'}
    digital_display = {'National Digital-Display', 'Local Digital-Display'}
    audio = {'Network Radio', 'Local Radio', 'Podcast'}

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
    elif mt in audio:
        return 'Audio'
    elif mt == 'Email':
        return 'Digital'
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
    """Read MediaRadar file with dynamic header detection."""
    filename = uploaded_file.name

    if filename.endswith('.csv'):
        raw = pd.read_csv(uploaded_file, header=None)
    else:
        raw = pd.read_excel(uploaded_file, sheet_name='Report Builder', header=None)

    # Find the row containing 'Format' to use as header
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


def process_mediaradar(mr_df, date_col, detected_optionals):
    """Process MediaRadar data: filter formats, unpivot months, map columns."""

    mr_df['Format'] = mr_df['Format'].str.strip()
    before = len(mr_df)
    mr_df = mr_df[mr_df['Format'].isin(INCLUDED_MEDIARADAR_FORMATS)]
    formats_excluded = before - len(mr_df)

    if mr_df.empty:
        return pd.DataFrame(), 0, formats_excluded

    # Identify month columns
    non_month_cols = ['Parent', 'Product Line', 'Format', 'Detailed Property',
                      'Media Property', 'National/Local', 'Market']
    month_cols = [c for c in mr_df.columns if c not in non_month_cols]

    # Unpivot monthly spend columns into rows
    mr_melted = mr_df.melt(
        id_vars=[c for c in non_month_cols if c in mr_df.columns],
        value_vars=month_cols,
        var_name='Month_Raw',
        value_name='Dollars'
    )

    # Drop rows with no spend
    mr_melted['Dollars'] = pd.to_numeric(mr_melted['Dollars'].astype(str).str.replace(
        r'[\$,]', '', regex=True), errors='coerce')
    mr_melted = mr_melted.dropna(subset=['Dollars'])
    mr_melted = mr_melted[mr_melted['Dollars'] != 0]

    if mr_melted.empty:
        return pd.DataFrame(), 0, formats_excluded

    # Parse month to datetime
    mr_melted[date_col] = pd.to_datetime(mr_melted['Month_Raw'], format='%b %Y', errors='coerce')
    if mr_melted[date_col].isna().all():
        mr_melted[date_col] = pd.to_datetime(mr_melted['Month_Raw'], errors='coerce')

    # Map format to media type, media category, middle media category
    mr_melted['Media Type'] = mr_melted['Format'].map(
        lambda x: MEDIARADAR_FORMAT_MAP.get(x, {}).get('Media Type', x))
    mr_melted['Media Category'] = mr_melted['Format'].map(
        lambda x: MEDIARADAR_FORMAT_MAP.get(x, {}).get('Media Category', 'N/A'))
    mr_melted['Middle Media Category'] = mr_melted['Format'].map(
        lambda x: MEDIARADAR_FORMAT_MAP.get(x, {}).get('Middle Media Category', 'N/A'))

    # Map columns
    mr_melted['Source'] = 'MediaRadar'
    mr_melted['Subsidiary'] = mr_melted.get('Parent', 'N/A')
    mr_melted['Brand Variant'] = mr_melted.get('Product Line', 'N/A')
    mr_melted['Distributor'] = mr_melted.get('Detailed Property', 'N/A')
    mr_melted['Distributor Description'] = mr_melted.get('Media Property', 'N/A')
    mr_melted['Market'] = mr_melted.get('Market', 'N/A')
    mr_melted['Commercial Duration'] = 'N/A'
    mr_melted['Estimated Impressions'] = 0
    mr_melted['Parent'] = mr_melted.get('Parent', 'N/A')

    # Map optional columns (handles Daypart -> 'N/A' automatically if detected elsewhere)
    mr_melted = map_optional_columns(mr_melted, 'MediaRadar', detected_optionals)

    return mr_melted, len(mr_melted), formats_excluded


def process_files(adintel_df, pathmatics_df, version, mr_df=None):
    is_weekly = 'weekly' in version
    include_impressions = 'impressions' in version
    date_col = 'Date' if is_weekly else 'Month'

    # ========== DETECT OPTIONAL COLUMNS ==========
    detected_optionals = detect_optional_columns(adintel_df, pathmatics_df, mr_df)

    # ========== ADINTEL DATES ==========
    if is_weekly:
        adintel_df['Date'] = adintel_df['Week'].str.split(' - ').str[0]
        adintel_df['Date'] = pd.to_datetime(adintel_df['Date'], format='%m/%d/%Y', errors='coerce')
    else:
        adintel_df['Month'] = pd.to_datetime(adintel_df['Month'], errors='coerce')
        adintel_df['Month'] = adintel_df['Month'].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)

    # Filter out Streaming
    streaming_removed = 0
    if 'Media Category' in adintel_df.columns:
        before = len(adintel_df)
        adintel_df = adintel_df[adintel_df['Media Category'].str.strip().str.lower() != 'streaming']
        streaming_removed = before - len(adintel_df)

    # Filter out Twitch.tv from AdIntel (Pathmatics Publisher data used instead)
    twitch_removed = 0
    if 'Distributor' in adintel_df.columns:
        before = len(adintel_df)
        adintel_df = adintel_df[adintel_df['Distributor'].str.strip().str.upper() != 'TWITCH.TV']
        twitch_removed = before - len(adintel_df)

    # Label YouTube distributors separately
    youtube_count = 0
    if 'Distributor' in adintel_df.columns:
        youtube_mask = adintel_df['Distributor'].str.strip().str.lower().str.contains('youtube', na=False)
        adintel_df.loc[youtube_mask, 'Media Type'] = 'YouTube (Digital Video)'
        youtube_count = youtube_mask.sum()

    # Detect brand column and save Brand Variant
    adintel_brand_col = detect_adintel_brand_col(adintel_df)
    if adintel_brand_col:
        adintel_df['Brand Variant'] = adintel_df[adintel_brand_col]

    # Detect Parent
    has_adintel_parent = 'Parent' in adintel_df.columns

    # Map optional columns for AdIntel (includes Daypart if detected)
    adintel_df = map_optional_columns(adintel_df, 'AdIntel', detected_optionals)

    # ========== PATHMATICS ==========

    # Exclude channels covered by AdIntel
    before_filter = len(pathmatics_df)
    pathmatics_df = pathmatics_df[~pathmatics_df['Channel'].str.strip().isin(EXCLUDED_PATHMATICS_CHANNELS)]
    channels_removed = before_filter - len(pathmatics_df)

    # Brand Variant from Brand Leaf
    if 'Brand Leaf' in pathmatics_df.columns:
        pathmatics_df['Brand Variant'] = pathmatics_df['Brand Leaf']
    elif 'Brand Root' in pathmatics_df.columns:
        pathmatics_df['Brand Variant'] = pathmatics_df['Brand Root']

    # Subsidiary = Brand Root
    if 'Brand Root' in pathmatics_df.columns:
        pathmatics_df['Subsidiary'] = pathmatics_df['Brand Root']

    # Parent = Advertiser
    if 'Advertiser' in pathmatics_df.columns:
        pathmatics_df['Parent'] = pathmatics_df['Advertiser']

    # Map optional columns for Pathmatics (Daypart -> 'N/A' automatically if detected)
    pathmatics_df = map_optional_columns(pathmatics_df, 'Pathmatics', detected_optionals)

    pathmatics_df['Date'] = pd.to_datetime(pathmatics_df['Date'], errors='coerce')

    if not is_weekly:
        pathmatics_df['Date'] = pathmatics_df['Date'] + pd.Timedelta(days=6)
        pathmatics_df['Date'] = pd.to_datetime(pathmatics_df['Date'].dt.strftime('%B %Y'))

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
        'Middle Media Category', 'Market',
        'Commercial Duration',
    ]
    if include_parent:
        base_columns.insert(1, 'Parent')

    # Insert optional columns before date column
    # (Daypart included here only if detected in AdIntel)
    for opt_col in detected_optionals:
        base_columns.append(opt_col)

    # Date and spend columns
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

    pathmatics_selected = pathmatics_df[base_columns]
    adintel_selected = adintel_df[base_columns]
    adintel_selected = adintel_selected.rename(columns={'Dollars ': 'Dollars'})

    frames = [pathmatics_selected, adintel_selected]

    # ========== PROCESS MEDIARADAR IF PROVIDED ==========
    mr_count = 0
    mr_formats_excluded = 0
    if mr_df is not None:
        mr_processed, mr_count, mr_formats_excluded = process_mediaradar(mr_df, date_col, detected_optionals)
        if not mr_processed.empty:
            for col in base_columns:
                if col not in mr_processed.columns:
                    mr_processed[col] = 'N/A'
            frames.append(mr_processed[base_columns])

    combined_df = pd.concat(frames, ignore_index=True)

    for col in combined_df.columns:
        if col != date_col:
            combined_df[col] = combined_df[col].fillna('N/A')

    combined_df['Media Type Grouped'] = combined_df['Media Type'].apply(group_media_type)

    return (
        combined_df, len(pathmatics_selected), len(adintel_selected),
        streaming_removed, channels_removed, youtube_count,
        mr_count, mr_formats_excluded, detected_optionals, twitch_removed
    )


# ========== STREAMLIT UI ==========

st.title("📊 Adintel + Pathmatics + MediaRadar Combiner")
st.markdown("""
Upload your files to automatically combine them.

**Methodology v4:**
- **AdIntel** → All traditional media + digital display/video + YouTube *(Twitch.tv excluded — use Pathmatics)*
- **Pathmatics** → Social media (FB, IG, TikTok, etc.) + OTT/CTV + Twitch only
- **MediaRadar** *(optional)* → Podcast, Email, Retail Media (Native) only
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
    | Market | `Market` | — (auto: NATIONAL) | `Market` |
    | Commercial Duration | `Commercial Duration` | `Duration` | — (N/A) |
    | Date | `Month` or `Week` | `Date` | Month columns (e.g., `Jan 2025`) |
    | Dollars | `Dollars` | `Spend (USD)` | Month columns (unpivoted) |

    ### Conditional Columns (appear when detected)

    | Output Column | AdIntel | Pathmatics | MediaRadar |
    |---|---|---|---|
    | Parent | `Parent` | `Advertiser` | `Parent` |
    | Estimated Impressions | `ImpE_P18_99` or `IMP_P2_99` | `Impressions` | — (N/A) |

    ### Optional Columns (appear when detected in any source)

    | Output Column | AdIntel | Pathmatics | MediaRadar |
    |---|---|---|---|
    | Landing Page | `Landing Page URL` | `Landing Page` | — |
    | Buy Type | `Buy Type` | `Ad Buy Type` | — |
    | Daypart | `Daypart` | — (N/A) | — (N/A) |
    | Device (Adintel) | `Device` | — | — |
    | Delivery Platform (Adintel) | `Delivery Platform` | — | — |
    | Placement (Pathmatics) | — | `Placement` | — |

    ### Auto-Generated Columns

    | Column | Description |
    |---|---|
    | Source | `AdIntel`, `Pathmatics`, or `MediaRadar` |
    | Middle Media Category | Mid-level grouping (Digital Video, Digital Display, Digital Social, OTT, Audio, etc.) |
    | Media Type Grouped | Top-level grouping (Digital Video, Social Media, Audio, Retail Media, etc.) |

    *MediaRadar is optional. Columns marked "—" are filled as N/A.*
    """)
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

                status = f"✅ Detected format: **{version_display}** | Brand column: **{brand_col or 'Not found'}**"
                if has_parent:
                    status += " | Parent: **detected**"
                if opt_cols:
                    status += f" | Optional columns: **{', '.join(opt_cols)}**"
                st.success(status)

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
                            mr_count, mr_fmt_excl, detected_opts, twitch_rm
                        ) = process_files(
                            adintel_df.copy(), pathmatics_df.copy(), version,
                            mr_df=mr_df.copy() if mr_df is not None else None
                        )
                        elapsed = (datetime.now() - start_time).total_seconds()

                    st.success(f"✅ Processing complete in {elapsed:.1f} seconds!")

                    result_cols = st.columns(4 if mr_df is not None else 3)
                    with result_cols[0]:
                        st.metric("Pathmatics (Social + OTT)", f"{path_count:,}")
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
                        st.write(f"**AdIntel Streaming rows removed:** {streaming_rm:,} (covered by Pathmatics OTT)")
                        st.write(f"**AdIntel Twitch.tv rows removed:** {twitch_rm:,} (covered by Pathmatics Publisher)")
                        st.write(f"**AdIntel YouTube rows relabeled:** {yt_count:,} → 'YouTube (Digital Video)'")
                        st.write(f"**Pathmatics rows excluded:** {channels_rm:,} (Desktop/Mobile Display/Video + YouTube)")
                        st.write("**Pathmatics channels kept:** Social platforms + OTT/CTV + Twitch")
                        if mr_df is not None:
                            st.write(f"**MediaRadar rows included:** {mr_count:,} (Podcast + Email + Retail Media)")
                            st.write(f"**MediaRadar rows excluded:** {mr_fmt_excl:,} (formats already covered by AdIntel/Pathmatics)")
                        if detected_opts:
                            st.write(f"**Optional columns detected:** {', '.join(detected_opts)}")

                    with st.expander("📊 Media Type Breakdown"):
                        source_summary = combined_df.groupby(['Source', 'Media Type Grouped'])['Dollars'].sum().reset_index()
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
*Methodology v4 — Auto-detects Weekly/Monthly, Impressions, Brand column, Parent column, and optional digital columns*
| Source | Covers | Optional Columns |
|--------|--------|-----------------|
| **AdIntel** | TV, Radio, Print, Outdoor, Digital Display, Digital Video, YouTube *(Twitch.tv excluded)* | Landing Page, Buy Type, Daypart, Device, Delivery Platform |
| **Pathmatics** | Social Media (FB, IG, TikTok, Snap, X, LinkedIn, Pinterest, Reddit) + OTT/CTV + Twitch | Landing Page, Buy Type, Placement |
| **MediaRadar** | Podcast → Audio, Email → Digital, Retail Media (Sponsored Shopping) | — |
""")
