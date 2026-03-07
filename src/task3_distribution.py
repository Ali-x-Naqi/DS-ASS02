import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy import stats

def execute_task3(df):
    """
    Task 3: Distribution Modeling & Tail Integrity (25%)
    Optimized peak vs tail (log-log CCDF).
    """
    # Preference for Industrial zone in a country with high readings (e.g. IN or US)
    ind_df = df[df['zone'] == 'Industrial']
    
    if ind_df.empty:
        # Fallback if no industrial zones found (unlikely with our heuristics)
        ind_df = df
        
    # Pick the country with the most data for the industrial plot
    target_country = ind_df['country'].value_counts().index[0]
    plot_df = ind_df[ind_df['country'] == target_country]
    
    pm_data = plot_df['pm25'].dropna().values
    
    if len(pm_data) < 10:
        return None, None, 0, 0, 0
    
    # 1. Peak-Optimized Plot (KDE)
    kde = stats.gaussian_kde(pm_data)
    x_peak = np.linspace(0, np.percentile(pm_data, 95), 500)
    y_peak = kde(x_peak)
    
    fig_peak = go.Figure()
    fig_peak.add_trace(go.Scatter(
        x=x_peak, y=y_peak,
        mode='lines', fill='tozeroy',
        line=dict(color="#3b82f6", width=2),
        name="Density"
    ))
    fig_peak.update_layout(
        title=f"<b>Distribution Mode: Peak-Optimized ({target_country} Industrial)</b>",
        xaxis_title="PM2.5 Concentration (µg/m³)",
        yaxis_title="Probability Density",
        template="plotly_white"
    )
    
    # 2. Tail-Optimized Plot (Log-Log CCDF)
    sorted_data = np.sort(pm_data[pm_data > 0]) # Log needs positive values
    ccdf = 1.0 - np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    
    fig_tail = go.Figure()
    fig_tail.add_trace(go.Scatter(
        x=sorted_data, y=ccdf,
        mode='lines',
        line=dict(color="#ef4444", width=3),
        name="CCDF"
    ))
    
    # Metrics
    p99 = np.percentile(pm_data, 99)
    max_val = np.max(pm_data)
    extreme_events = np.sum(pm_data > 200)
    
    fig_tail.add_vline(x=p99, line_dash="dash", line_color="orange")
    
    fig_tail.update_layout(
        title=f"<b>Distribution Mode: Tail-Optimized ({target_country} Industrial)</b>",
        xaxis_type="log", yaxis_type="log",
        xaxis_title="Log(PM2.5 Concentration)",
        yaxis_title="Log(Probability > x)",
        template="plotly_white"
    )
    
    return fig_peak, fig_tail, p99, max_val, extreme_events
