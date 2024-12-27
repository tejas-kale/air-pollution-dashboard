"""Page rendering functions for the Streamlit app."""

import streamlit as st
from datetime import datetime, timedelta
from utils import load_cities, load_pollutant_config
from data import (
    get_annual_means,
    get_rolling_means,
    get_o3_peak_season,
    get_o3_rolling
)
from plots import (
    plot_annual_means,
    plot_rolling_means,
    plot_o3_peak_season,
    plot_o3_rolling
)

# Load pollutant configuration
POLLUTANT_INFO = load_pollutant_config()

def render_landing_page():
    """Render the landing page content."""
    st.title("🌍 Air Pollution Dashboard")
    
    st.markdown("""
    Welcome to the Air Pollution Dashboard! This application provides comprehensive 
    visualizations of air quality data across multiple cities, focusing on key air pollutants 
    and their comparison to WHO Air Quality Guidelines.
    
    ### Available Pollutants
    - **Particulate Matter**
        - PM2.5 (Fine particles ≤ 2.5 μm)
        - PM10 (Coarse particles ≤ 10 μm)
    - **Gaseous Pollutants**
        - NO₂ (Nitrogen Dioxide)
        - SO₂ (Sulfur Dioxide)
        - CO (Carbon Monoxide)
        - O₃ (Ozone)
    
    ### Key Features
    - **Multiple Visualizations**
        - Annual mean concentrations (PM2.5, PM10, NO₂)
        - 24-hour rolling averages (all pollutants)
        - WHO guideline reference lines
    
    - **Interactive Elements**
        - Date range selection
        - City comparisons
        - Expandable pollutant information
    
    - **Data Quality Metrics**
        - Measurement completeness
        - Data coverage periods
        - Real-time updates
    """)
    
    # Add data overview
    st.subheader("Current Data Coverage")
    cities = load_cities()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Monitored Cities")
        st.markdown(f"""
        - Total cities: **{len(cities)}**
        - Locations: {', '.join(cities)}
        """)
    
    with col2:
        st.markdown("#### Measurement Frequency")
        st.markdown("""
        - Hourly measurements
        - 24-hour rolling averages
        - Annual mean calculations
        """)
    
    # Add WHO guidelines reference
    st.markdown("""
    ---
    ### WHO Air Quality Guidelines (2021)
    
    | Pollutant | Annual Mean | 24-hour Mean |
    |-----------|-------------|--------------|
    | PM2.5 | 5 μg/m³ | 15 μg/m³ |
    | PM10 | 15 μg/m³ | 45 μg/m³ |
    | NO₂ | 10 μg/m³ | 25 μg/m³ |
    | SO₂ | - | 40 μg/m³ |
    | CO | - | 4 mg/m³ |
    | O₃ | - | 100 μg/m³ |
    
    *Source: [WHO Global Air Quality Guidelines](https://www.who.int/publications/i/item/9789240034228)*
    """)

def render_pollutant_page(pollutant):
    """
    Render visualizations for a specific pollutant.
    
    Args:
        pollutant (str): Name of the pollutant
    """
    st.header(f"{pollutant} Concentrations")
    
    # Show pollutant description
    with st.expander("About this pollutant"):
        st.markdown(POLLUTANT_INFO[pollutant]["description"])
    
    # For pollutants with annual means (PM2.5, PM10, NO2)
    if "annual_col" in POLLUTANT_INFO[pollutant]:
        # Fetch and display annual data
        with st.spinner("Loading annual means..."):
            annual_data = get_annual_means()
        
        st.subheader("Annual Mean Concentrations")
        st.plotly_chart(
            plot_annual_means(annual_data, pollutant, load_cities()),
            use_container_width=True
        )
    
    # For all pollutants - show rolling means
    st.subheader("24-hour Rolling Mean Concentrations")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=7),
            max_value=datetime.now(),
            key=f"{pollutant}_start_date"
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now(),
            max_value=datetime.now(),
            min_value=start_date,
            key=f"{pollutant}_end_date"
        )
    
    # Convert dates to datetime
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())
    
    # Update rolling means data based on selection
    with st.spinner("Loading rolling means..."):
        rolling_data = get_rolling_means(start_datetime, end_datetime)
    
    if rolling_data.empty:
        st.warning("No data available for the selected date range.")
    else:
        # Display rolling means chart
        st.plotly_chart(
            plot_rolling_means(rolling_data, pollutant, load_cities()),
            use_container_width=True
        )
        
        # Add data quality information
        st.subheader("Data Quality")
        st.markdown(f"""
        - Date range: {rolling_data['timestamp'].min()} to {rolling_data['timestamp'].max()}
        - Data completeness: {rolling_data['data_completeness_pct'].mean():.1f}% average
        """)

def render_o3_page():
    """Render O3-specific visualizations."""
    st.header("O₃ Concentrations")
    
    # Show pollutant description
    with st.expander("About this pollutant"):
        st.markdown(POLLUTANT_INFO["O3"]["description"])
    
    # Peak Season Analysis
    st.subheader("Peak Season Analysis")
    with st.spinner("Loading peak season data..."):
        peak_season_data = get_o3_peak_season()
    
    if peak_season_data.empty:
        st.warning("No peak season data available.")
    else:
        st.plotly_chart(
            plot_o3_peak_season(peak_season_data, load_cities()),
            use_container_width=True
        )
    
    # 8-hour Rolling Maximum
    st.subheader("8-hour Rolling Maximum")
    with st.spinner("Loading rolling maximum data..."):
        rolling_data = get_o3_rolling()
    
    if rolling_data.empty:
        st.warning("No rolling maximum data available.")
    else:
        st.plotly_chart(
            plot_o3_rolling(rolling_data, load_cities()),
            use_container_width=True
        )
        
        # Add data quality information
        st.subheader("Data Quality")
        st.markdown(f"""
        - Date range: {rolling_data['date'].min()} to {rolling_data['date'].max()}
        - Number of measurements: {len(rolling_data)}
        """) 