import pandas as pd
import numpy as np
import plotly.graph_objects as go
from scipy import stats

def execute_task3(df):
    """
    Task 3: Distribution Modeling & Tail Integrity (25%)
    Optimized peak vs tail (log-log CCDF). 99th percentile, >200 events.
    """
    # Focusing on an Industrial zone (e.g., in IN where we injected the max)
    industrial_df = df[(df['zone'] == 'Industrial') & (df['country'] == 'IN')]
    pm_data = industrial_df['pm25'].values
    
    # 1. Peak-Optimized Plot (KDE)
    kde = stats.gaussian_kde(pm_data)
    x_peak = np.linspace(0, 150, 500)
    y_peak = kde(x_peak)
    
    fig_peak = go.Figure()
    fig_peak.add_trace(go.Scatter(
        x=x_peak, y=y_peak,
        mode='lines', fill='tozeroy',
        line=dict(color="#3b82f6", width=2),
        name="Density"
    ))
    fig_peak.update_layout(
        title="<b>Distribution Mode: Peak-Optimized (Linear KDE)</b><br>Hides the extreme tail due to scale crush.",
        xaxis_title="PM2.5 Concentration (µg/m³)",
        yaxis_title="Probability Density",
        template="plotly_white",
        font=dict(family="Inter", size=12),
        margin=dict(t=50)
    )
    
    # 2. Tail-Optimized Plot (Log-Log CCDF)
    sorted_data = np.sort(pm_data)
    ccdf = 1.0 - np.arange(1, len(sorted_data) + 1) / len(sorted_data)
    
    fig_tail = go.Figure()
    fig_tail.add_trace(go.Scatter(
        x=sorted_data, y=ccdf,
        mode='lines',
        line=dict(color="#ef4444", width=3),
        name="CCDF"
    ))
    
    # Calculate required metrics for the narrative
    p99 = np.percentile(pm_data, 99)
    max_val = np.max(pm_data)
    extreme_events = np.sum(pm_data > 200)
    
    fig_tail.add_vline(x=p99, line_dash="dash", line_color="orange")
    fig_tail.add_annotation(x=np.log10(p99), y=np.log10(0.01), text=f"P99: {p99:.1f}", showarrow=True, arrowhead=1)
    
    fig_tail.update_layout(
        title="<b>Distribution Mode: Tail-Optimized (Log-Log CCDF)</b><br>Reveals the true extent of rare, extreme hazard events.",
        xaxis_type="log", yaxis_type="log",
        xaxis_title="Log(PM2.5 Concentration)",
        yaxis_title="Log(Probability > x)",
        template="plotly_white",
        font=dict(family="Inter", size=12),
        margin=dict(t=50)
    )
    
    return fig_peak, fig_tail, p99, max_val, extreme_events
