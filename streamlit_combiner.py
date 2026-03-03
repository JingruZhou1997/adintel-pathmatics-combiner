"""
Adintel + Pathmatics Combiner - Streamlit App
Deploy to Streamlit Cloud for free: https://streamlit.io/cloud

Methodology v3:
- AdIntel: Keep ALL data (traditional + digital display/video + YouTube)
- Pathmatics: Add only Social Media and OTT/CTV (exclude Desktop/Mobile Display/Video and YouTube)
- AdIntel Streaming: Excluded (Pathmatics OTT has broader coverage)
- YouTube: Labeled separately as 'YouTube (Digital Video)' from AdIntel distributor data

To run locally:
    pip install streamlit pandas openpyxl
    streamlit run streamlit_combiner.py
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime

st.set_page_config(page_title="Adintel + Pathmatics Combiner", page_icon="📊", layout="wide")

# ========== CHANNELS TO EXCLUDE FROM PATHMATICS ==========
EXCLUDED_PATHMATICS_CHANNELS = {
    'Desktop Display', 'Desktop Video', 'Mobile Display', 'Mobile Video', 'YouTube'
}


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
    return row.get('Media Category', 'N/A')


def group_media_type(media_type):
    mt = str(media_type).strip()
    social = {'Facebook', 'Instagram', 'Snapchat', 'TikTok',
              'X', 'Twitter', 'LinkedIn', 'Pinterest', 'Reddit'}
    spanish_tv = {'Spanish Language Cable TV', 'Spanish Language Network TV'}
    clearance_tv = {'Network Clearance Spot TV', 'Syndicated Clearance Spot TV'}
    digital_video = {'National Digital-Video', 'Local Digital-Video', 'YouTube (Digital Video)'}
    digital_display = {'National Digital-Display', 'Local Digital-Display'}

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


def process_files(adintel_df, pathmatics_df, version):
    is_weekly = 'weekly' in version
    include_impressions = 'impressions' in version
    date_col = 'Date' if is_weekly else 'Month'

    # ========== ADINTEL DATES ==========
    if is_weekly:
        adintel_df['Date'] = adintel_df['Week'].str.split(' - ').str[0]
        adintel_df['Date'] = pd.to_datetime(adintel_df['Date'], format='%m/%d/%Y', errors='coerce')
    else:
        adintel_df['Month'] = pd.to_datetime(adintel_df['Month'], errors='coerce')
        adintel_df['Month'] = adintel_df['Month'].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)

    # Filter out Streaming (Pathmatics OTT has broader coverage)
    streaming_removed = 0
    if 'Media Category' in adintel_df.columns:
        before = len(adintel_df)
        adintel_df = adintel_df[adintel_df['Media Category'].str.strip().str.lower() != 'streaming']
        streaming_removed = before - len(adintel_df)

    # Label YouTube distributors separately
    youtube_count = 0
    if 'Distributor' in adintel_df.columns:
        youtube_mask = adintel_df['Distributor'].str.strip().str.lower().str.contains('youtube', na=False)
        adintel_df.loc[youtube_mask, 'Media Type'] = 'YouTube (Digital Video)'
        youtube_count = youtube_mask.sum()

    # Detect brand column and save Brand Variant before renaming
    adintel_brand_col = detect_adintel_brand_col(adintel_df)
    if adintel_brand_col:
        adintel_df['Brand Variant'] = adintel_df[adintel_brand_col]

    # ========== PATHMATICS ==========

    # Exclude channels covered by AdIntel
    before_filter = len(pathmatics_df)
    pathmatics_df = pathmatics_df[~pathmatics_df['Channel'].str.strip().isin(EXCLUDED_PATHMATICS_CHANNELS)]
    channels_removed = before_filter - len(pathmatics_df)

    # Save Brand Variant before renaming
    if 'Brand Root' in pathmatics_df.columns:
        pathmatics_df['Brand Variant'] = pathmatics_df['Brand Root']

    pathmatics_df['Date'] = pd.to_datetime(pathmatics_df['Date'], errors='coerce')

    if not is_weekly:
        pathmatics_df['Date'] = pathmatics_df['Date'] + pd.Timedelta(days=6)
        pathmatics_df['Date'] = pd.to_datetime(pathmatics_df['Date'].dt.strftime('%B %Y'))

    pathmatics_df['Media Category'] = 'Digital'
    pathmatics_df['Market'] = 'NATIONAL'
    pathmatics_df['Source'] = 'Pathmatics'
    pathmatics_df['Daypart'] = 'N/A'
    pathmatics_df['Distributor Description'] = 'N/A'
    pathmatics_df['Duration'] = pathmatics_df['Duration'].fillna('N/A')

    if 'Impressions' not in pathmatics_df.columns:
        pathmatics_df['Impressions'] = 0

    # Assign middle category BEFORE renaming Channel
    pathmatics_df['Middle Media Category'] = pathmatics_df['Channel'].apply(assign_pathmatics_middle_category)

    pathmatics_df.rename(columns={
        'Brand Root': 'Brand Name',
        'Channel': 'Media Type',
        'Duration': 'Commercial Duration',
        'Publisher': 'Distributor',
        'Spend (USD)': 'Dollars',
        'Advertiser': 'Subsidiary',
        'Impressions': 'Estimated Impressions',
        'Date': date_col,
    }, inplace=True)

    # ========== ADINTEL FOR COMBINATION ==========
    adintel_df['Source'] = 'AdIntel'
    adintel_df['Middle Media Category'] = adintel_df.apply(assign_adintel_middle_category, axis=1)

    rename_map = {adintel_brand_col: 'Brand Name'} if adintel_brand_col else {}
    if include_impressions:
        if 'ImpE_P18_99' in adintel_df.columns:
            rename_map['ImpE_P18_99'] = 'Estimated Impressions'
        elif 'IMP_P2_99' in adintel_df.columns:
            rename_map['IMP_P2_99'] = 'Estimated Impressions'
        else:
            adintel_df['Estimated Impressions'] = 0

    if rename_map:
        adintel_df.rename(columns=rename_map, inplace=True)

    # ========== COMBINE ==========
    base_columns = [
        'Source', 'Subsidiary', 'Brand Name', 'Brand Variant', 'Distributor',
        'Distributor Description', 'Media Type', 'Media Category',
        'Middle Media Category', 'Market', 'Daypart',
        'Commercial Duration', date_col, 'Dollars'
    ]
    if include_impressions:
        base_columns.append('Estimated Impressions')

    # Ensure Brand Variant exists in both
    if 'Brand Variant' not in pathmatics_df.columns:
        pathmatics_df['Brand Variant'] = pathmatics_df.get('Brand Name', 'N/A')
    if 'Brand Variant' not in adintel_df.columns:
        adintel_df['Brand Variant'] = adintel_df.get('Brand Name', 'N/A')

    pathmatics_selected = pathmatics_df[base_columns]
    adintel_selected = adintel_df[base_columns]
    adintel_selected = adintel_selected.rename(columns={'Dollars ': 'Dollars'})

    combined_df = pd.concat([pathmatics_selected, adintel_selected], ignore_index=True)

    for col in combined_df.columns:
        if col != date_col:
            combined_df[col] = combined_df[col].fillna('N/A')

    combined_df['Media Type Grouped'] = combined_df['Media Type'].apply(group_media_type)

    return combined_df, len(pathmatics_selected), len(adintel_selected), streaming_removed, channels_removed, youtube_count


# ========== STREAMLIT UI ==========

st.title("📊 Adintel + Pathmatics Combiner")
st.markdown("""
Upload your Adintel and Pathmatics files to automatically combine them.

**Methodology v3:**
- **AdIntel** → All traditional media + digital display/video + YouTube
- **Pathmatics** → Social media (FB, IG, TikTok, etc.) + OTT/CTV only
- No overlap between sources
""")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Adintel File")
    adintel_file = st.file_uploader("Upload Adintel file", type=['csv', 'xlsx'], key='adintel')

with col2:
    st.subheader("Pathmatics File")
    pathmatics_file = st.file_uploader("Upload Pathmatics file", type=['csv', 'xlsx'], key='pathmatics')

if adintel_file and pathmatics_file:
    with st.spinner("Reading files..."):
        try:
            adintel_df = read_adintel(adintel_file)
            pathmatics_df = read_pathmatics(pathmatics_file)
            version, version_display = detect_version(adintel_df)

            if version is None:
                st.error("❌ Could not detect file format. Make sure Adintel file has 'Week' or 'Month' column.")
            else:
                # Detect brand column
                brand_col = detect_adintel_brand_col(adintel_df)
                st.success(f"✅ Detected format: **{version_display}** | Brand column: **{brand_col or 'Not found'}**")

                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Adintel Rows", f"{len(adintel_df):,}")
                with col2:
                    st.metric("Pathmatics Rows", f"{len(pathmatics_df):,}")

                if st.button("🚀 Process & Combine", type="primary"):
                    with st.spinner("Processing..."):
                        start_time = datetime.now()
                        combined_df, path_count, adin_count, streaming_rm, channels_rm, yt_count = process_files(
                            adintel_df.copy(), pathmatics_df.copy(), version
                        )
                        elapsed = (datetime.now() - start_time).total_seconds()

                    st.success(f"✅ Processing complete in {elapsed:.1f} seconds!")

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Pathmatics Rows (Social + OTT)", f"{path_count:,}")
                    with col2:
                        st.metric("Adintel Rows (All media)", f"{adin_count:,}")
                    with col3:
                        st.metric("Combined Total", f"{len(combined_df):,}")

                    with st.expander("📋 Processing Details"):
                        st.write(f"**AdIntel Streaming rows removed:** {streaming_rm:,} (covered by Pathmatics OTT)")
                        st.write(f"**AdIntel YouTube rows relabeled:** {yt_count:,} → 'YouTube (Digital Video)'")
                        st.write(f"**Pathmatics rows excluded:** {channels_rm:,} (Desktop/Mobile Display/Video + YouTube)")
                        st.write("**Pathmatics channels kept:** Social platforms + OTT/CTV")

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
    st.info("👆 Please upload both files to begin.")

st.markdown("---")
st.markdown("""
*Methodology v3 — Auto-detects Weekly/Monthly, Impressions, and Brand column formats*
| Source | Covers |
|--------|--------|
| **AdIntel** | TV, Radio, Print, Outdoor, Digital Display, Digital Video, YouTube |
| **Pathmatics** | Social Media (FB, IG, TikTok, Snap, X, LinkedIn, Pinterest, Reddit) + OTT/CTV |
""")
