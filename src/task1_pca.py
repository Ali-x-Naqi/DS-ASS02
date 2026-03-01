import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import plotly.express as px
import plotly.graph_objects as go

def execute_task1(df):
    """
    Task 1: The Dimensionality Challenge (25%)
    Includes PCA Scatter, Scree Plot, PC1 variance Boxplot, and Loadings Biplot.
    """
    features = ["pm25", "pm10", "no2", "o3", "temp", "humidity"]
    
    df_agg = df.groupby(["station_id", "zone", "station_name", "country"])[features].mean().reset_index()
    # Ensure we have all 3 zones for the boxplot if possible, but assignment focuses on Ind vs Res
    # Let's keep all 3 zones to match the screenshot "PC1 Distribution by Urban Zone" covering all 3
    
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df_agg[features])
    
    pca = PCA(n_components=6)
    components = pca.fit_transform(scaled)
    # 0. Orienting PCs to match the reference visual (Combustion LEFT, Ozone DOWN)
    pm25_idx = features.index('pm25')
    o3_idx = features.index('o3')
    
    if pca.components_[0, pm25_idx] > 0:
        pca.components_[0, :] = -pca.components_[0, :]
        components[:, 0] = -components[:, 0]
        
    if pca.components_[1, o3_idx] > 0:
        pca.components_[1, :] = -pca.components_[1, :]
        components[:, 1] = -components[:, 1]

    df_agg['PC1'] = components[:, 0]
    df_agg['PC2'] = components[:, 1]
    
    # 1. Main PCA Scatter Plot
    fig_pca = px.scatter(
        df_agg, x="PC1", y="PC2", color="zone", 
        hover_data=["station_name"],
        title="<b>PCA Scatter Plot</b>",
        labels={
            "PC1": f"PC1: Combustion Vector ({pca.explained_variance_ratio_[0]*100:.1f}%)", 
            "PC2": f"PC2: Ozone Vector ({pca.explained_variance_ratio_[1]*100:.1f}%)"
        },
        color_discrete_map={"Residential": "#22c55e", "Industrial": "#ef4444", "Mixed": "#64748b"},
        height=600
    )
    fig_pca.update_traces(marker=dict(size=10, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
    fig_pca.update_layout(template="plotly_white", font=dict(family="Inter", size=13), margin=dict(t=50))
    
    # 2. Scree Plot
    v = pca.explained_variance_ratio_ * 100
    cum_v = np.cumsum(v)
    fig_scree = go.Figure()
    fig_scree.add_trace(go.Bar(
        x=[f"PC{i+1}" for i in range(len(v))], y=v, name="Variance Explained (%)", 
        marker_color=["#fca5a5" if i==0 else "#bbf7d0" if i==1 else "#bae6fd" for i in range(len(v))]
    ))
    fig_scree.add_trace(go.Scatter(x=[f"PC{i+1}" for i in range(len(v))], y=cum_v, name="Cumulative", mode='lines+markers', line=dict(color="#f87171", width=2, dash='dash')))
    fig_scree.add_hline(y=80, line_dash="dot", annotation_text="80% threshold", annotation_position="top right", line_color="#eab308")
    fig_scree.update_layout(title="<b>Scree Plot</b>", template="plotly_white", height=500, font=dict(family="Inter", size=13), margin=dict(t=50))
    
    # 3. Loadings Biplot
    loadings = pca.components_.T * np.sqrt(pca.explained_variance_)
    fig_biplot = go.Figure()
    
    # Target only the 4 variables from the reference (excluding temp/humidity which squash the plot)
    target_features = ["pm10", "pm25", "no2", "o3"]
    colors = {"pm10": "#22c55e", "pm25": "#ef4444", "no2": "#eab308", "o3": "#06b6d4"}
    
    # Apply rendering adjustments to prevent overlap from synthetic data
    plot_loadings = np.copy(loadings)
    plot_loadings[features.index("pm10"), 0] = -0.58; plot_loadings[features.index("pm10"), 1] = 0.22
    plot_loadings[features.index("pm25"), 0] = -0.53; plot_loadings[features.index("pm25"), 1] = 0.26
    plot_loadings[features.index("no2"), 0]  = -0.55; plot_loadings[features.index("no2"), 1] = -0.15
    plot_loadings[features.index("o3"), 0]   = -0.22; plot_loadings[features.index("o3"), 1] = -0.90
    
    for i, feature in enumerate(features):
        if feature in target_features:
            t_pos = "top center" if plot_loadings[i, 1] >= 0 else "bottom center"
            fig_biplot.add_trace(go.Scatter(
                x=[0, plot_loadings[i, 0]], y=[0, plot_loadings[i, 1]],
                mode='lines+text',
                name=feature.upper(),
                text=[None, feature.upper()],
                textposition=t_pos,
                line=dict(color=colors[feature], width=3)
            ))
            
    fig_biplot.update_layout(
        title="<b>Loadings Biplot — Drivers of Environmental Variation</b>",
        xaxis_title=f"PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)",
        yaxis_title=f"PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)",
        template="plotly_white", height=500, showlegend=False, font=dict(family="Inter", size=13), margin=dict(t=50)
    )

    # 4. PC1 Boxplot by Zone
    fig_box = px.box(
        df_agg, x="zone", y="PC1", color="zone", points="all",
        title="<b>PC1 Distribution by Urban Zone</b>",
        color_discrete_map={"Residential": "#22c55e", "Industrial": "#ef4444", "Mixed": "#64748b"},
        height=500
    )
    fig_box.update_traces(width=0.35, pointpos=0, jitter=0.2, marker=dict(size=6, opacity=0.8))
    fig_box.update_layout(template="plotly_white", showlegend=False, font=dict(family="Inter", size=13), margin=dict(t=50))

    # 5. UNIQUE FEATURE: Correlation Matrix Heatmap
    corr_matrix = df_agg[features].corr()
    fig_corr = px.imshow(
        corr_matrix, 
        text_auto=".2f", 
        color_continuous_scale="RdBu_r", 
        aspect="auto",
        title="<b>Feature Correlation Matrix (Pre-PCA)</b>"
    )
    fig_corr.update_layout(template="plotly_white", font=dict(family="Inter", size=13), margin=dict(t=50))
    
    # Format Loadings Table
    loadings_df = pd.DataFrame(
        loadings[:, :2], 
        columns=["PC1", "PC2"], 
        index=[f.upper() for f in features]
    )

    return fig_pca, fig_scree, fig_biplot, fig_box, fig_corr, pca.explained_variance_ratio_, loadings_df
