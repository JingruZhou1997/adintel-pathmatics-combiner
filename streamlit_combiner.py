"""
Adintel + Pathmatics Combiner - Streamlit App
Deploy to Streamlit Cloud for free: https://streamlit.io/cloud

To run locally:
    pip install streamlit pandas openpyxl
    streamlit run streamlit_combiner.py
"""

import streamlit as st
import pandas as pd
import io
from datetime import datetime

st.set_page_config(page_title="Adintel + Pathmatics Combiner", page_icon="📊", layout="wide")

# ========== HELPER FUNCTIONS ==========

def detect_version(adintel_df):
    """Detect which version based on columns present."""
    columns = [col.strip() for col in adintel_df.columns.tolist()]
    
    has_week = 'Week' in columns
    has_month = 'Month' in columns
    has_impressions = 'IMP_P2_99' in columns
    
    if has_week and has_impressions:
        return 'weekly_impressions', 'Weekly + Impressions'
    elif has_week:
        return 'weekly', 'Weekly'
    elif has_month and has_impressions:
        return 'monthly_impressions', 'Monthly + Impressions'
    elif has_month:
        return 'monthly', 'Monthly'
    else:
        return None, 'Unknown'

def modify_channel(value):
    if value in ["Desktop Display", "Mobile Display"]:
        return "Digital Display"
    elif value in ["Desktop Video", "Mobile Video"]:
        return "Digital Video"
    return value

def group_media_type(media_type):
    media_type_str = str(media_type).strip()
    social = ['Facebook', 'Instagram', 'Snapchat', 'TikTok']
    spanish_tv = ['Spanish Language Cable TV', 'Spanish Language Network TV']
    
    if media_type_str in social:
        return 'Social Media'
    elif media_type_str in spanish_tv:
        return 'Spanish Language TV'
    return media_type_str

def read_adintel(uploaded_file):
    """Read Adintel file (CSV or Excel)."""
    filename = uploaded_file.name
    if filename.endswith('.csv'):
        df = pd.read_csv(uploaded_file, skiprows=2)
    else:
        df = pd.read_excel(uploaded_file, sheet_name='Report', skiprows=3)
    df.columns = df.columns.str.strip()
    return df

def read_pathmatics(uploaded_file):
    """Read Pathmatics file (CSV or Excel)."""
    filename = uploaded_file.name
    if filename.endswith('.csv'):
        df = pd.read_csv(uploaded_file, skiprows=1)
    else:
        df = pd.read_excel(uploaded_file, skiprows=1)
    df.columns = df.columns.str.strip()
    return df

def process_files(adintel_df, pathmatics_df, version):
    """Process and combine the files."""
    
    is_weekly = 'weekly' in version
    include_impressions = 'impressions' in version
    date_col = 'Date' if is_weekly else 'Month'
    
    # ========== PROCESS ADINTEL DATES ==========
    if is_weekly:
        adintel_df['Date'] = adintel_df['Week'].str.split(' - ').str[0]
        adintel_df['Date'] = pd.to_datetime(adintel_df['Date'], format='%m/%d/%Y', errors='coerce')
    else:
        adintel_df['Month'] = pd.to_datetime(adintel_df['Month'], errors='coerce')
        adintel_df['Month'] = adintel_df['Month'].apply(lambda x: x.replace(day=1) if pd.notnull(x) else x)
    
    # Filter out Streaming
    if 'Media Category' in adintel_df.columns:
        adintel_df = adintel_df[adintel_df['Media Category'].str.strip().str.lower() != 'streaming']
    
    # ========== PROCESS PATHMATICS ==========
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
    
    pathmatics_df['Channel'] = pathmatics_df['Channel'].apply(modify_channel)
    pathmatics_df['Middle Media Category'] = pathmatics_df['Channel'].apply(
        lambda x: 'OTT' if str(x).strip() == 'OTT' else 'Digital'
    )
    
    pathmatics_df.rename(columns={
        'Brand Root': 'Brand Name',
        'Channel': 'Media Type',
        'Duration': 'Commercial Duration',
        'Publisher': 'Distributor',
        'Spend (USD)': 'Dollars',
        'Advertiser': 'Subsidiary',
        'Date': date_col
    }, inplace=True)
    
    # ========== PROCESS ADINTEL FOR COMBINATION ==========
    def check_combined(row):
        mc = str(row['Media Category']).strip().lower()
        mkt = str(row['Market']).strip().lower()
        return 'NO' if mc == 'digital' and mkt == 'national' else 'YES'
    
    adintel_df['Combined or not'] = adintel_df.apply(check_combined, axis=1)
    adintel_df['Source'] = 'AdIntel'
    adintel_df['Middle Media Category'] = adintel_df['Media Category']
    
    rename_map = {'Brand Variant': 'Brand Name'}
    if include_impressions:
        rename_map['IMP_P2_99'] = 'Impressions'
    adintel_df.rename(columns=rename_map, inplace=True)
    
    adintel_df = adintel_df[adintel_df['Combined or not'] == 'YES']
    
    # ========== SELECT COLUMNS AND COMBINE ==========
    base_columns = [
        'Source', 'Subsidiary', 'Brand Name', 'Distributor', 'Distributor Description',
        'Media Type', 'Media Category', 'Middle Media Category', 'Market', 'Daypart',
        'Commercial Duration', date_col, 'Dollars'
    ]
    
    if include_impressions:
        base_columns.append('Impressions')
    
    pathmatics_selected = pathmatics_df[base_columns]
    adintel_selected = adintel_df[base_columns]
    
    combined_df = pd.concat([pathmatics_selected, adintel_selected], ignore_index=True)
    
    for col in combined_df.columns:
        if col != date_col:
            combined_df[col] = combined_df[col].fillna('N/A')
    
    combined_df['Media Type Grouped'] = combined_df['Media Type'].apply(group_media_type)
    
    return combined_df, len(pathmatics_selected), len(adintel_selected)


# ========== STREAMLIT UI ==========

st.title("📊 Adintel + Pathmatics Combiner")
st.markdown("Upload your Adintel and Pathmatics files to automatically combine them.")

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
                st.success(f"✅ Detected format: **{version_display}**")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Adintel Rows", f"{len(adintel_df):,}")
                with col2:
                    st.metric("Pathmatics Rows", f"{len(pathmatics_df):,}")
                
                if st.button("🚀 Process & Combine", type="primary"):
                    with st.spinner("Processing... This may take a moment for large files."):
                        start_time = datetime.now()
                        
                        combined_df, path_count, adin_count = process_files(
                            adintel_df.copy(), 
                            pathmatics_df.copy(), 
                            version
                        )
                        
                        elapsed = (datetime.now() - start_time).total_seconds()
                    
                    st.success(f"✅ Processing complete in {elapsed:.1f} seconds!")
                    
                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Pathmatics Rows", f"{path_count:,}")
                    with col2:
                        st.metric("Adintel Rows", f"{adin_count:,}")
                    with col3:
                        st.metric("Combined Total", f"{len(combined_df):,}")
                    
                    # Preview
                    st.subheader("Preview (first 100 rows)")
                    st.dataframe(combined_df.head(100), use_container_width=True)
                    
                    # Download button
                    csv_buffer = io.StringIO()
                    combined_df.to_csv(csv_buffer, index=False)
                    csv_data = csv_buffer.getvalue()
                    
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    filename = f"Combined_{version_display.replace(' + ', '_').replace(' ', '')}_{timestamp}.csv"
                    
                    st.download_button(
                        label="📥 Download Combined CSV",
                        data=csv_data,
                        file_name=filename,
                        mime="text/csv",
                        type="primary"
                    )
                    
        except Exception as e:
            st.error(f"❌ Error processing files: {str(e)}")
            st.exception(e)

else:
    st.info("👆 Please upload both files to begin.")

# Footer
st.markdown("---")
st.markdown("*Auto-detects Weekly/Monthly and Impressions formats*")
