import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def execute_overview(df):
    """
    Generates a Global Overview Map and summary metrics.
    """
    # Group by station to get 100 points
    nodes = df.groupby(["station_name", "country", "zone", "lat", "lon"])["pm25"].mean().reset_index()
    
    # 1. Global Spatial Map
    fig_map = px.scatter_mapbox(
        nodes, lat="lat", lon="lon",
        color="pm25", size="pm25",
        hover_name="station_name",
        hover_data=["country", "zone"],
        color_continuous_scale="Agsunset",
        size_max=15, zoom=1,
        title="<b>Global Sensor Deployment Map (2025)</b>"
    )
    fig_map.update_layout(
        mapbox_style="carto-darkmatter",
        template="plotly_dark",
        margin={"r":0,"t":50,"l":0,"b":0},
        title_x=0.05,
        height=600,
        font=dict(family="Inter", color="white")
    )
    
    return fig_map, len(nodes), len(df)
