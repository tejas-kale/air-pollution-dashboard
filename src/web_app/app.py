"""
Streamlit App for Air Pollution Visualization

This app displays visualizations of air pollution data from BigQuery,
showing various pollutant measurements and their comparison to WHO guidelines.
"""

import streamlit as st
from src.utils.bq_utils import load_environment
from pages import render_landing_page, render_pollutant_page, render_o3_page

def main():
    """Main function to run the Streamlit app."""
    try:
        # Load environment variables
        load_environment()
        
        # Set up page config
        st.set_page_config(
            page_title="Air Pollution Dashboard",
            page_icon="🌍",
            layout="wide"
        )
        
        # Create tabs with formatted names
        tabs = st.tabs([
            "Overview",
            "PM₂.₅",  # Using unicode subscript
            "PM₁₀",   # Using unicode subscript
            "NO₂",    # Using unicode subscript
            "SO₂",    # Using unicode subscript
            "CO",
            "O₃"      # Using unicode subscript
        ])
        
        # Render content for each tab
        with tabs[0]:
            render_landing_page()
            
        # Render pollutant pages with formatted names
        pollutants = ["PM2.5", "PM10", "NO2", "SO2", "CO"]  # Keep internal names unchanged
        for i, pollutant in enumerate(pollutants, 1):
            with tabs[i]:
                render_pollutant_page(pollutant)
        
        # Render O3 page separately since it has a different structure
        with tabs[6]:
            render_o3_page()
            
    except Exception as e:
        st.error(f"Error initializing app: {str(e)}")

if __name__ == "__main__":
    main()
