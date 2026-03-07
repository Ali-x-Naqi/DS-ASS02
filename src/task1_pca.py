import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import plotly.express as px
import plotly.graph_objects as go

def execute_task1(df):
    """
    Task 1: The Dimensionality Challenge (25%)
    Performs PCA and returns analysis figures.
    """
    # 1. Targeted Cleaning
    features = ['pm25', 'pm10', 'no2', 'o3', 'temp', 'humidity']
    
    # Aggregate by station
    avg_df = df.groupby(['station_id', 'station_name', 'zone', 'country', 'pop_density'])[features].mean().reset_index()
    
    # Drop rows with any NaN in features for PCA
    pca_ready = avg_df.dropna(subset=features)
    
    if pca_ready.empty:
        # Fallback: Impute with global mean
        pca_ready = avg_df.copy()
        for f in features:
            pca_ready[f] = pca_ready[f].fillna(df[f].mean())
    
    X = pca_ready[features]
    
    # 2. Standarization & PCA
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    pca = PCA(n_components=min(len(features), len(pca_ready)))
    pca_transformed = pca.fit_transform(X_scaled)
    
    # Create Variance Scree Plot
    variance = pca.explained_variance_ratio_
    fig_scree = px.bar(
        x=[f"PC{i+1}" for i in range(len(variance))],
        y=variance * 100,
        labels={"x": "Principal Component", "y": "Variance Explained (%)"},
        title="<b>Scree Plot: Variance Integrity Audit</b>",
        template="plotly_white"
    )
    
    # Create PCA Scatter Plot
    pca_df = pd.DataFrame(pca_transformed[:, :2], columns=['PC1', 'PC2'])
    pca_df = pd.concat([pca_df, pca_ready[['station_name', 'zone', 'country']].reset_index(drop=True)], axis=1)
    
    # Orient PC1 positively for pollution
    if pca.components_[0, 0] < 0:
        pca_df['PC1'] *= -1
    
    fig_pca = px.scatter(
        pca_df, x='PC1', y='PC2', color='zone',
        hover_data=['station_name', 'country'],
        title="<b>PCA Station Clustering (Environmental State Space)</b>",
        template="plotly_white",
        color_discrete_map={"Industrial": "#ef4444", "Residential": "#10b981"}
    )
    
    # Create Biplot (Loadings)
    loadings = pca.components_.T * np.sqrt(pca.explained_variance_)
    fig_biplot = go.Figure()
    for i, feature in enumerate(features):
        fig_biplot.add_trace(go.Scatter(
            x=[0, loadings[i, 0]], y=[0, loadings[i, 1]],
            mode='lines+text', name=feature.upper(),
            text=[None, feature.upper()], textposition="top center",
            line=dict(width=3)
        ))
    fig_biplot.update_layout(title="<b>Axis Loadings: Drivers of Urban Separation</b>", template="plotly_white")
    
    # Box Plot for PC1 separation
    fig_box = px.box(
        pca_df, x='zone', y='PC1', color='zone',
        title="<b>PC1 Separation: Industrial vs Residential Clusters</b>",
        template="plotly_white",
        color_discrete_map={"Industrial": "#ef4444", "Residential": "#10b981"}
    )
    
    # Correlation Heatmap
    corr = df[features].corr()
    fig_corr = px.imshow(
        corr, text_auto=".2f",
        title="<b>Multi-Collinearity Audit: Feature Relationships</b>",
        color_continuous_scale="RdBu_r"
    )

    loadings_df = pd.DataFrame(loadings[:, :2], index=[f.upper() for f in features], columns=['PC1', 'PC2'])

    return fig_pca, fig_scree, fig_biplot, fig_box, fig_corr, variance, loadings_df
