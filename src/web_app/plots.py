"""Plotting functions for the Streamlit app."""

import plotly.express as px
import plotly.graph_objects as go
from utils import load_pollutant_config

# Load pollutant configuration
POLLUTANT_INFO = load_pollutant_config()

def plot_annual_means(df, pollutant, cities):
    """
    Create a bar chart for annual means with reference lines.
    
    Args:
        df (pd.DataFrame): Annual means data
        pollutant (str): Pollutant name
        cities (list): List of cities to plot
        
    Returns:
        plotly.graph_objects.Figure: The plotted figure
    """
    col = POLLUTANT_INFO[pollutant]["annual_col"]
    
    # Create the bar chart with reduced opacity
    fig = px.bar(
        df,
        x="city",
        y=col,
        facet_col="year",
        title=f"Annual Mean {pollutant} Concentrations by City",
        labels={
            col: f'Concentration ({POLLUTANT_INFO[pollutant]["unit"]})',
            "city": "City",
            "year": "Year",
        },
        opacity=0.7
    )
    
    # Add reference line
    reference_value = POLLUTANT_INFO[pollutant]["annual_ref"]
    
    # Add a horizontal line to each subplot
    for i in range(len(fig.data)):
        fig.add_hline(
            y=reference_value,
            line_dash="dash",
            line_color="red",
            annotation_text=f"WHO Guideline: {reference_value} {POLLUTANT_INFO[pollutant]['unit']}",
            annotation_position="top right",
            line_width=1.5,
            opacity=0.9,
            row=1,
            col=i + 1
        )
    
    fig.update_layout(
        height=500,
        showlegend=False,
        margin=dict(t=50),
        annotations=[
            dict(font=dict(color="rgba(0,0,0,0.8)"))
            for _ in fig.layout.annotations
        ]
    )
    
    return fig

def plot_rolling_means(df, pollutant, cities):
    """
    Create a line chart for rolling means with reference line.
    
    Args:
        df (pd.DataFrame): Rolling means data
        pollutant (str): Pollutant name
        cities (list): List of cities to plot
        
    Returns:
        plotly.graph_objects.Figure: The plotted figure
    """
    col = POLLUTANT_INFO[pollutant]["rolling_col"]
    
    fig = go.Figure()
    
    # Create traces in order of cities list
    for city in cities:
        if city in df['city'].unique():
            city_data = df[df['city'] == city]
            fig.add_trace(go.Scatter(
                x=city_data['timestamp'],
                y=city_data[col],
                name=city,
                mode='lines',
                hovertemplate=(
                    "Date: %{x}<br>"
                    f"{pollutant}: %{{y:.1f}} {POLLUTANT_INFO[pollutant]['unit']}<br>"
                    "<extra></extra>"
                )
            ))
    
    # Add reference line
    reference_value = POLLUTANT_INFO[pollutant]["daily_ref"]
    
    fig.add_hline(
        y=reference_value,
        line_dash="dash",
        line_color="red",
        annotation=dict(
            text=f"WHO Guideline: {reference_value} {POLLUTANT_INFO[pollutant]['unit']}",
            xanchor="right",
            x=1,
            yanchor="bottom",
            y=reference_value,
            font=dict(color="rgba(0,0,0,0.8)")
        ),
        line_width=1.5,
        opacity=0.9
    )
    
    fig.update_layout(
        title=f"24-hour Rolling Mean {pollutant} Concentrations",
        xaxis_title="Date",
        yaxis_title=f"Concentration ({POLLUTANT_INFO[pollutant]['unit']})",
        height=600,
        showlegend=True,
        legend_title_text="City",
        margin=dict(r=150)
    )
    
    return fig

def plot_o3_peak_season(df, cities):
    """
    Create a scatter plot for peak season O3 concentrations.
    
    Args:
        df (pd.DataFrame): Peak season O3 data
        cities (list): List of cities to plot
        
    Returns:
        plotly.graph_objects.Figure: The plotted figure
    """
    fig = go.Figure()
    
    # Create traces in order of cities list
    for city in cities:
        if city in df['city'].unique():
            city_data = df[df['city'] == city]
            fig.add_trace(go.Scatter(
                x=city_data['date'],
                y=city_data['daily_max_o3_8h'],
                name=city,
                mode='markers',
                marker=dict(
                    size=8,
                    opacity=0.7,
                    line=dict(width=1, color='white')
                ),
                hovertemplate=(
                    "Date: %{x}<br>"
                    "O₃: %{y:.1f} μg/m³<br>"
                    "<extra></extra>"
                )
            ))
    
    # Add reference line
    fig.add_hline(
        y=POLLUTANT_INFO["O3"]["peak_season_ref"],
        line_dash="dash",
        line_color="red",
        annotation=dict(
            text=f"WHO Guideline: {POLLUTANT_INFO['O3']['peak_season_ref']} μg/m³",
            xanchor="right",
            x=1,
            yanchor="bottom",
            y=POLLUTANT_INFO["O3"]["peak_season_ref"],
            font=dict(color="rgba(0,0,0,0.8)")
        ),
        line_width=1.5,
        opacity=0.9
    )
    
    fig.update_layout(
        title="Peak Season 8-hour Maximum O₃ Concentrations",
        xaxis_title="Date",
        yaxis_title="Concentration (μg/m³)",
        height=600,
        showlegend=True,
        legend_title_text="City",
        margin=dict(r=150),
        xaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(0,0,0,0.1)'
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(0,0,0,0.1)'
        )
    )
    
    return fig

def plot_o3_rolling(df, cities):
    """
    Create a line chart for 8-hour rolling maximum O3 concentrations.
    
    Args:
        df (pd.DataFrame): Rolling O3 data
        cities (list): List of cities to plot
        
    Returns:
        plotly.graph_objects.Figure: The plotted figure
    """
    fig = go.Figure()
    
    # Create traces in order of cities list
    for city in cities:
        if city in df['city'].unique():
            city_data = df[df['city'] == city]
            fig.add_trace(go.Scatter(
                x=city_data['date'],
                y=city_data['daily_max_o3_8h'],
                name=city,
                mode='lines',
                hovertemplate=(
                    "Date: %{x}<br>"
                    "O₃: %{y:.1f} μg/m³<br>"
                    "<extra></extra>"
                )
            ))
    
    # Add reference line
    fig.add_hline(
        y=POLLUTANT_INFO["O3"]["rolling_ref"],
        line_dash="dash",
        line_color="red",
        annotation=dict(
            text=f"WHO Guideline: {POLLUTANT_INFO['O3']['rolling_ref']} μg/m³",
            xanchor="right",
            x=1,
            yanchor="bottom",
            y=POLLUTANT_INFO["O3"]["rolling_ref"],
            font=dict(color="rgba(0,0,0,0.8)")
        ),
        line_width=1.5,
        opacity=0.9
    )
    
    fig.update_layout(
        title="8-hour Rolling Maximum O₃ Concentrations",
        xaxis_title="Date",
        yaxis_title="Concentration (μg/m³)",
        height=600,
        showlegend=True,
        legend_title_text="City",
        margin=dict(r=150)
    )
    
    return fig 