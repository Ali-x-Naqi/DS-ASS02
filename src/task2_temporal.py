import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

def execute_task2(df):
    """
    Task 2: High-Density Temporal Analysis (25%)
    Creates a detailed monthly percentage heatmap.
    """
    df_clean = df.copy()
    if 'pm25' not in df_clean.columns:
        return None, None, 0
        
    df_clean = df_clean.dropna(subset=['pm25'])
    
    # 1. Prepare Monthly Violation Data
    df_clean['month'] = df_clean['timestamp'].dt.to_period('M')
    
    # Calculate total readings and violations per station per month
    monthly_stats = df_clean.groupby(['station_name', 'month']).agg(
        total_readings=('pm25', 'count'),
        violations=('pm25', lambda x: (x > 35).sum())
    ).reset_index()
    
    # Calculate Violation Percentage
    monthly_stats['violation_pct'] = (monthly_stats['violations'] / monthly_stats['total_readings']) * 100
    
    # Pivot for Heatmap
    pivot = monthly_stats.pivot(index='station_name', columns='month', values='violation_pct').fillna(0)
    
    # Sort stations by total average violation to put worst ones at the bottom (most visible)
    pivot['avg_sort'] = pivot.mean(axis=1)
    pivot = pivot.sort_values('avg_sort').drop('avg_sort', axis=1)
    
    # Format labels
    z_values = pivot.values
    x_labels = [str(m) for m in pivot.columns]
    y_labels = pivot.index.tolist()
    z_text = np.vectorize(lambda x: f"{x:.1f}%")(z_values)
    
    huge_height = max(800, len(pivot) * 20)

    # 2. Build the Annotated Plotly Heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_values, x=x_labels, y=y_labels,
        text=z_text, texttemplate="%{text}",
        textfont={"size": 10},
        colorscale="OrRd",
        showscale=True,
        xgap=1, ygap=1,
        colorbar=dict(title="Violation %", thickness=15)
    ))
    
    fig.update_layout(
        title="<b>Temporal Violation Heatmap (PM2.5 > 35 µg/m³)</b>",
        xaxis_title="Month (2025)",
        yaxis_title="Monitoring Station",
        template="plotly_white",
        height=huge_height,
        font=dict(family="Inter", size=11)
    )
    
    # 3. Global Trend
    monthly_aggregate = monthly_stats.groupby('month')['violation_pct'].mean().reset_index()
    monthly_aggregate['month_str'] = monthly_aggregate['month'].astype(str)
    
    fig_line = px.line(
        monthly_aggregate, x='month_str', y='violation_pct',
        title="<b>Global Monthly Average Violation Rate</b>",
        template="plotly_white",
        height=400,
        markers=True
    )
    fig_line.update_traces(line=dict(color="#ef4444", width=3))
    
    violation_rate = (df_clean['pm25'] > 35).mean() * 100
    
    # 4. Compute Dynamic Extremes for the Dashboard Text
    worst_row = monthly_stats.loc[monthly_stats['violation_pct'].idxmax()]
    best_row = monthly_stats.loc[monthly_stats['violation_pct'].idxmin()]
    
    volatility = monthly_stats.groupby('station_name')['violation_pct'].std().reset_index()
    most_volatile = volatility.loc[volatility['violation_pct'].idxmax()]['station_name']
    
    extremes = {
        "highest": f"{worst_row['station_name']} ({worst_row['violation_pct']:.1f}%)",
        "lowest": f"{best_row['station_name']} ({best_row['violation_pct']:.1f}%)",
        "volatile": most_volatile
    }
    
    return fig, fig_line, violation_rate, extremes
