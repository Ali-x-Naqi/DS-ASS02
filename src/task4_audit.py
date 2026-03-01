import pandas as pd
import numpy as np
import plotly.express as px

def execute_task4(df):
    """
    Task 4: The Visual Integrity Audit (25%)
    Rejects 3D Bar. Uses Small Multiples per region, X=pop density, Y=pm2.5, color=Viridis.
    """
    # Aggregate by station for the scatter plot
    agg = df.groupby(['station_name', 'country', 'pop_density'])['pm25'].mean().reset_index()
    
    # We use 'country' as the 'region' for the small multiples
    fig_multiples = px.scatter(
        agg, x="pop_density", y="pm25", color="pm25",
        facet_col="country", facet_col_wrap=4,
        color_continuous_scale="Viridis",
        title="<b>Tufte-Compliant Mapping: PM2.5 vs Population Density (By Region)</b>",
        labels={"pop_density": "Population Density", "pm25": "Mean PM2.5", "country": "Region"},
        height=800
    )
    
    fig_multiples.update_layout(
        template="plotly_white",
        font=dict(family="Inter", size=12),
        coloraxis_colorbar=dict(title="PM2.5 Level"),
        margin=dict(t=50)
    )
    
    return fig_multiples
