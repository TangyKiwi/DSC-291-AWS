"""
Folium Map Visualization for HW2 Part 3

Visualize PC1 and PC2 scores on an interactive Folium map of NYC taxi zones.
"""

import pickle
import numpy as np
import pandas as pd
import geopandas as gpd
import folium
from folium import plugins
from branca.colormap import LinearColormap
import dask.dataframe as dd
from pathlib import Path


def load_pca_model(model_path: str):
    """
    Load PCA model.
    
    Args:
        model_path: Path to pca_model.pkl
        
    Returns:
        components: PCA components matrix
        mean: Mean vector used for centering
    """
    print(f"Loading PCA model from {model_path}...")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    components = model['components']  # Shape: (24, 24)
    mean = model['mean']  # Shape: (24,)
    
    print(f"Loaded PCA model: {components.shape[0]} components, {components.shape[1]} features")
    
    return components, mean


def load_data_and_compute_scores(parquet_path: str, components: np.ndarray, mean: np.ndarray):
    """
    Load original data and compute PCA scores.
    
    Args:
        parquet_path: Path to taxi_wide_table.parquet
        components: PCA components matrix
        mean: Mean vector for centering
        
    Returns:
        scores_df: DataFrame with pickup_place, pc1, pc2
    """
    print(f"Loading data from {parquet_path}...")
    
    # Load parquet file
    df = dd.read_parquet(parquet_path, engine='pyarrow')
    
    # Extract hour columns
    hour_cols = [f'hour_{i}' for i in range(24)]
    X_df = df[hour_cols]
    
    # Get metadata
    metadata_cols = ['taxi_type', 'date', 'pickup_place']
    metadata_df = df[metadata_cols].compute() if all(col in df.columns for col in metadata_cols) else None
    
    # Convert to numpy
    X_pandas = X_df.compute()
    X = X_pandas.values  # Shape: (n_samples, 24)
    
    print(f"Loaded {X.shape[0]} samples")
    
    # Center data
    X_centered = X - mean
    
    # Compute PCA scores: scores = X_centered @ components.T
    scores = X_centered @ components.T  # Shape: (n_samples, 24)
    
    # Extract PC1 and PC2
    pc1 = scores[:, 0]
    pc2 = scores[:, 1]
    
    # Combine with metadata
    scores_df = pd.DataFrame({
        'pickup_place': metadata_df['pickup_place'].values if metadata_df is not None else None,
        'pc1': pc1,
        'pc2': pc2
    })
    
    # Remove rows with missing pickup_place
    if metadata_df is not None:
        scores_df = scores_df.dropna(subset=['pickup_place'])
    
    print(f"Computed scores for {len(scores_df)} rows")
    print(f"PC1 range: [{pc1.min():.2f}, {pc1.max():.2f}]")
    print(f"PC2 range: [{pc2.min():.2f}, {pc2.max():.2f}]")
    
    return scores_df


def aggregate_scores_by_place(scores_df: pd.DataFrame):
    """
    Aggregate PC scores by pickup_place using mean.
    
    Args:
        scores_df: DataFrame with pickup_place, pc1, pc2
        
    Returns:
        agg_df: Aggregated DataFrame with pickup_place, pc1_mean, pc2_mean
    """
    print("Aggregating scores by pickup_place...")
    
    agg_df = scores_df.groupby('pickup_place').agg({
        'pc1': 'mean',
        'pc2': 'mean'
    }).reset_index()
    
    agg_df.columns = ['pickup_place', 'pc1_mean', 'pc2_mean']
    
    print(f"Aggregated to {len(agg_df)} unique pickup places")
    
    return agg_df


def load_taxi_zones(shapefile_path: str):
    """
    Load taxi zone shapefile and convert to WGS84.
    
    Args:
        shapefile_path: Path to taxi_zones.shx or .shp
        
    Returns:
        zones_gdf: GeoDataFrame with zone geometries
    """
    print(f"Loading taxi zones from {shapefile_path}...")
    
    # Load shapefile
    zones_gdf = gpd.read_file(shapefile_path)
    
    # Convert CRS to WGS84 (EPSG:4326) if needed
    if zones_gdf.crs is None:
        # Assume it's in EPSG:2263 (NY State Plane) based on codebase
        zones_gdf = zones_gdf.set_crs(epsg=2263)
    
    if zones_gdf.crs.to_epsg() != 4326:
        zones_gdf = zones_gdf.to_crs(epsg=4326)
    
    print(f"Loaded {len(zones_gdf)} zones")
    print(f"CRS: {zones_gdf.crs}")
    print(f"Columns: {zones_gdf.columns.tolist()}")
    
    return zones_gdf


def join_scores_with_zones(agg_scores: pd.DataFrame, zones_gdf: gpd.GeoDataFrame):
    """
    Join aggregated PC scores with zone geometries.
    
    Args:
        agg_scores: DataFrame with pickup_place, pc1_mean, pc2_mean
        zones_gdf: GeoDataFrame with zone geometries
        
    Returns:
        merged_gdf: GeoDataFrame with scores and geometries
    """
    print("Joining scores with zone geometries...")
    
    # Based on codebase: pickup_place = zone_index + 1 (1-indexed)
    # So zones are 0-indexed by their position in the GeoDataFrame
    # Create zone_id from index (0-indexed)
    zones_gdf = zones_gdf.reset_index(drop=True)
    zones_gdf['zone_index'] = zones_gdf.index  # 0-indexed
    
    # pickup_place is 1-indexed, so match: zone_index = pickup_place - 1
    agg_scores['zone_index'] = agg_scores['pickup_place'] - 1
    
    # Merge on zone_index
    merged = zones_gdf.merge(
        agg_scores,
        left_on='zone_index',
        right_on='zone_index',
        how='inner'
    )
    
    # Drop the temporary zone_index column if desired
    # merged = merged.drop(columns=['zone_index'])
    
    print(f"Successfully joined {len(merged)} zones with PC scores")
    
    return merged


def create_folium_map(merged_gdf: gpd.GeoDataFrame, output_path: str):
    """
    Create Folium map with PC1 as color and PC2 as opacity.
    
    Args:
        merged_gdf: GeoDataFrame with geometries, pc1_mean, pc2_mean
        output_path: Path to save HTML file
    """
    print("Creating Folium map...")
    
    # Calculate center of NYC (approximate)
    bounds = merged_gdf.total_bounds
    center_lat = (bounds[1] + bounds[3]) / 2
    center_lon = (bounds[0] + bounds[2]) / 2
    
    # Initialize map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles='OpenStreetMap'
    )
    
    # Normalize PC1 for color mapping
    pc1_min = merged_gdf['pc1_mean'].min()
    pc1_max = merged_gdf['pc1_mean'].max()
    pc1_range = pc1_max - pc1_min
    
    # Normalize PC2 for opacity (use range [0.3, 1.0] to avoid fully transparent)
    pc2_min = merged_gdf['pc2_mean'].min()
    pc2_max = merged_gdf['pc2_mean'].max()
    pc2_range = pc2_max - pc2_min
    
    # Create colormap for PC1 (using RdYlBu diverging colormap)
    colormap = LinearColormap(
        colors=['#313695', '#4575b4', '#74add1', '#abd9e9', '#e0f3f8', 
                '#ffffcc', '#fee090', '#fdae61', '#f46d43', '#d73027', '#a50026'],
        vmin=pc1_min,
        vmax=pc1_max,
        caption='PC1 Score'
    )
    
    # Add colormap to map
    colormap.add_to(m)
    
    # Function to get color from PC1 value
    def get_color(pc1_val):
        return colormap(pc1_val)
    
    # Function to get opacity from PC2 value
    def get_opacity(pc2_val):
        if pc2_range == 0:
            return 0.7
        normalized = (pc2_val - pc2_min) / pc2_range
        # Map to [0.3, 1.0] range
        return 0.3 + normalized * 0.7
    
    # Convert to GeoJSON string
    geojson_data = merged_gdf.to_json()
    
    # Style function that uses both PC1 (color) and PC2 (opacity)
    def style_function(feature):
        props = feature['properties']
        pc1_val = props.get('pc1_mean', 0)
        pc2_val = props.get('pc2_mean', 0)
        return {
            'fillColor': get_color(pc1_val),
            'fillOpacity': get_opacity(pc2_val),
            'color': 'black',
            'weight': 1,
            'opacity': 0.7
        }
    
    # Add GeoJSON layer with style function
    folium.GeoJson(
        geojson_data,
        style_function=style_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['pickup_place', 'pc1_mean', 'pc2_mean'],
            aliases=['Zone:', 'PC1:', 'PC2:'],
            localize=True
        )
    ).add_to(m)
    
    # Save map
    m.save(output_path)
    print(f"Map saved to {output_path}")


def main():
    """Main execution function."""
    # Define paths
    base_dir = Path(__file__).parent
    model_path = base_dir / 'pca_model.pkl'
    parquet_path = base_dir / 'data' / 'input' / 'taxi_wide_table.parquet'
    shapefile_path = base_dir.parent / 'pivot_and_bootstrap' / 'taxi_zones.shx'
    output_path = base_dir / 'pc1_pc2_folium_map.html'
    
    # Step 1: Load PCA model
    components, mean = load_pca_model(str(model_path))
    
    # Step 2: Load data and compute PCA scores
    scores_df = load_data_and_compute_scores(str(parquet_path), components, mean)
    
    # Step 3: Aggregate scores by pickup_place
    agg_scores = aggregate_scores_by_place(scores_df)
    
    # Step 4: Load taxi zones
    zones_gdf = load_taxi_zones(str(shapefile_path))
    
    # Step 5: Join scores with zones
    merged_gdf = join_scores_with_zones(agg_scores, zones_gdf)
    
    # Step 6: Create and save Folium map
    create_folium_map(merged_gdf, str(output_path))
    
    print("\nFolium map creation complete!")
    print(f"Map saved to: {output_path}")


if __name__ == '__main__':
    main()
