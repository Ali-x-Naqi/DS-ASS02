import pandas as pd
import plotly.graph_objects as go
import numpy as np
import plotly.figure_factory as ff

def execute_task2(df):
    """
    Task 2: High-Density Temporal Analysis (25%)
    Creates a detailed monthly percentage heatmap to match the requested granular style.
    """
    # 1. Prepare Monthly Violation Data
    df['month'] = df['timestamp'].dt.to_period('M')
    
    # Calculate total readings and violations per station per month
    monthly_stats = df.groupby(['station_name', 'month']).agg(
        total_readings=('pm25', 'count'),
        violations=('pm25', lambda x: (x > 35).sum())
    ).reset_index()
    
    # Calculate Violation Percentage
    monthly_stats['violation_pct'] = (monthly_stats['violations'] / monthly_stats['total_readings']) * 100
    
    # Pivot for Heatmap (Rows: Stations, Columns: Months)
    pivot = monthly_stats.pivot(index='station_name', columns='month', values='violation_pct').fillna(0)
    
    # Order the pivot to put Coyhaique II at the bottom for emphasis (as seen in screenshot)
    stations = pivot.index.tolist()
    if 'Coyhaique II' in stations:
        stations.append(stations.pop(stations.index('Coyhaique II')))
        pivot = pivot.loc[stations]
    
    # Format labels
    z_values = pivot.values
    x_labels = [str(m) for m in pivot.columns]
    y_labels = pivot.index.tolist()
    
    # Create annotation text (e.g. "15.3%")
    z_text = np.vectorize(lambda x: f"{x:.1f}%")(z_values)
    
    huge_height = max(800, len(pivot) * 28) # 28 pixels per row gives good spacing

    # 2. Build the Annotated Plotly Heatmap
    fig = go.Figure(data=go.Heatmap(
        z=z_values,
        x=x_labels,
        y=y_labels,
        text=z_text,
        texttemplate="%{text}",
        textfont={"size": 10, "color": "black"},
        colorscale=[
            [0.0, "#ffffcc"],   # Very pale yellow (0%)
            [0.1, "#ffeda0"],   # Light yellow
            [0.3, "#fed976"],   # Yellow-orange
            [0.5, "#feb24c"],   # Orange
            [0.7, "#fd8d3c"],   # Dark orange
            [0.9, "#f03b20"],   # Bright red
            [1.0, "#bd0026"]    # Deep dark red (100%)
        ],
        showscale=True,
        xgap=1, ygap=1, # Add grid lines
        colorbar=dict(
            title="Violation Pct %",
            thickness=15,
            len=0.9,
            yanchor="middle",
            y=0.5
        )
    ))
    
    fig.update_layout(
        title="", # Removed title inside graph to match screenshot
        xaxis_title="Month",
        yaxis_title="Monitoring Station",
        template="plotly_white",
        height=huge_height,
        font=dict(family="Roboto", size=11, color="#333333"),
        margin=dict(l=150, r=20, t=10, b=50),
        xaxis=dict(
            tickmode='array', 
            tickvals=x_labels, 
            ticktext=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][:len(x_labels)],
            side="bottom"
        ),
        yaxis=dict(
            autorange="reversed" # To match typical reading order if needed, but keeping Coyhaique II at bottom is fine.
        )
    )
    
    # 3. UNIQUE FEATURE: Add a secondary aggregate line chart to add visual density
    monthly_aggregate = monthly_stats.groupby('month')['violation_pct'].mean().reset_index()
    monthly_aggregate['month_str'] = monthly_aggregate['month'].astype(str)
    
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=monthly_aggregate['month_str'], 
        y=monthly_aggregate['violation_pct'],
        mode='lines+markers',
        line=dict(color="#bd0026", width=3),
        marker=dict(size=8, color="#fd8d3c"),
        name="Global Average"
    ))
    fig_line.update_layout(
        title="<b>Global Monthly Average Violation Rate</b>",
        xaxis_title="Month",
        yaxis_title="Average Violation %",
        template="plotly_white",
        height=350,
        margin=dict(l=50, r=20, t=50, b=50)
    )
    
    # Calculate metrics matching the insight
    violation_rate = (df['pm25'] > 35).mean() * 100
    
    return fig, fig_line, violation_rate
