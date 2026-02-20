"""
PCA Implementation for HW2 Part 1

Fit PCA on unnormalized wide table using Dask for covariance computation.
"""

import pickle
import numpy as np
import dask.array as da
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


def load_data(parquet_path: str):
    """
    Load parquet file and extract feature matrix X.
    
    Args:
        parquet_path: Path to the parquet file
        
    Returns:
        X_dask: Dask array of shape (n_samples, 24) with hour_0..hour_23 columns
        metadata_df: DataFrame with (taxi_type, date, pickup_place) for reference
    """
    print(f"Loading data from {parquet_path}...")
    
    # Load parquet file using Dask
    df = dd.read_parquet(parquet_path, engine='pyarrow')
    
    # Extract hour columns (hour_0 through hour_23)
    hour_cols = [f'hour_{i}' for i in range(24)]
    
    # Extract feature matrix X
    X_df = df[hour_cols]
    
    # Keep metadata for reference
    metadata_cols = ['taxi_type', 'date', 'pickup_place']
    metadata_df = df[metadata_cols].compute() if all(col in df.columns for col in metadata_cols) else None
    
    # Convert to Dask array
    # Compute to pandas first, then convert to dask array for better control
    X_pandas = X_df.compute()
    X_dask = da.from_array(X_pandas.values, chunks='auto')
    
    print(f"Loaded data: {X_dask.shape[0]} samples, {X_dask.shape[1]} features")
    
    return X_dask, metadata_df


def handle_missing_values(X_dask: da.Array):
    """
    Replace missing values with column means (excluding missing values).
    
    Args:
        X_dask: Dask array of shape (n_samples, n_features)
        
    Returns:
        X_filled: Dask array with missing values replaced by column means
        mean_vector: NumPy array of column means
    """
    print("Handling missing values...")
    
    # Compute column means excluding missing values
    # Convert to numpy for efficient nanmean computation
    X_numpy = X_dask.compute()
    mean_vector = np.nanmean(X_numpy, axis=0)
    
    # Replace NaN with column means using vectorized operations
    # Create a mask for NaN values
    nan_mask = np.isnan(X_numpy)
    # Use np.where to replace NaNs with means
    X_filled = np.where(nan_mask, mean_vector, X_numpy)
    
    # Check if there were any missing values
    n_missing = np.sum(nan_mask)
    if n_missing > 0:
        print(f"Replaced {n_missing} missing values with column means")
    else:
        print("No missing values found")
    
    # Convert back to Dask array for covariance computation
    X_filled_dask = da.from_array(X_filled, chunks='auto')
    
    return X_filled_dask, mean_vector


def compute_covariance_matrix(X_dask: da.Array, mean_vector: np.ndarray):
    """
    Compute covariance matrix using Dask, averaging outer products only once.
    
    Args:
        X_dask: Dask array of shape (n_samples, n_features)
        mean_vector: NumPy array of shape (n_features,) with column means
        
    Returns:
        cov_matrix: NumPy array of shape (n_features, n_features) - covariance matrix
    """
    print("Computing covariance matrix with Dask...")
    
    # Convert mean_vector to Dask array for broadcasting
    mean_dask = da.from_array(mean_vector, chunks=mean_vector.shape)
    
    # Center the data: X_centered = X - mean
    X_centered = X_dask - mean_dask
    
    # Get number of samples
    n_samples = X_dask.shape[0]
    
    # Compute covariance: C = (1/(n-1)) * X_centered.T @ X_centered
    # This computes the average of outer products in one operation
    # X_centered.T @ X_centered computes sum of outer products, then we divide by (n-1)
    cov_dask = da.dot(X_centered.T, X_centered) / (n_samples - 1)
    
    # Materialize the result
    cov_matrix = cov_dask.compute()
    
    print(f"Covariance matrix shape: {cov_matrix.shape}")
    print(f"Covariance matrix computed")
    
    # Verify symmetry (should be symmetric up to numerical precision)
    if not np.allclose(cov_matrix, cov_matrix.T):
        print("Warning: Covariance matrix is not symmetric (within tolerance)")
    
    return cov_matrix


def perform_pca(cov_matrix: np.ndarray):
    """
    Perform PCA via eigenvalue decomposition.
    
    Args:
        cov_matrix: Covariance matrix of shape (n_features, n_features)
        
    Returns:
        components: Orthonormal eigenvectors (rows = components, columns = features)
        explained_variance: Eigenvalues in descending order
    """
    print("Performing eigenvalue decomposition...")
    
    # Perform eigenvalue decomposition: C = V @ diag(λ) @ V.T
    # Use eigh for symmetric matrices (more stable)
    eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)
    
    # Sort by eigenvalues in descending order
    idx = np.argsort(eigenvalues)[::-1]
    eigenvalues_sorted = eigenvalues[idx]
    eigenvectors_sorted = eigenvectors[:, idx]
    
    # Ensure eigenvectors are normalized (they should be from eigh, but verify)
    # Each column is an eigenvector, normalize to unit length
    norms = np.linalg.norm(eigenvectors_sorted, axis=0)
    eigenvectors_normalized = eigenvectors_sorted / norms
    
    # Transpose so rows = components, columns = features (standard PCA format)
    components = eigenvectors_normalized.T
    
    print(f"Eigenvalue decomposition complete")
    print(f"Number of components: {components.shape[0]}")
    print(f"Top 5 eigenvalues: {eigenvalues_sorted[:5]}")
    
    return components, eigenvalues_sorted


def save_model(components: np.ndarray, explained_variance: np.ndarray, 
               mean: np.ndarray, output_path: str):
    """
    Save PCA model as pickle file.
    
    Args:
        components: Orthonormal eigenvectors (rows = components, columns = features)
        explained_variance: Eigenvalues in descending order
        mean: Column means used for centering
        output_path: Path to save the pickle file
    """
    print(f"Saving model to {output_path}...")
    
    model_dict = {
        'components': components,
        'explained_variance': explained_variance,
        'mean': mean
    }
    
    with open(output_path, 'wb') as f:
        pickle.dump(model_dict, f)
    
    print(f"Model saved successfully")


def visualize_variance_explained(explained_variance: np.ndarray, output_path: str):
    """
    Create variance explained visualization.
    
    Args:
        explained_variance: Eigenvalues in descending order
        output_path: Path to save the PNG file
    """
    print(f"Creating variance explained plot...")
    
    n_components = len(explained_variance)
    component_numbers = np.arange(1, n_components + 1)
    
    # Compute cumulative variance explained percentage
    total_variance = np.sum(explained_variance)
    cumulative_variance = np.cumsum(explained_variance)
    cumulative_percentage = (cumulative_variance / total_variance) * 100
    
    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Scree plot: eigenvalues vs component number
    ax1.plot(component_numbers, explained_variance, 'bo-', linewidth=2, markersize=6)
    ax1.set_xlabel('Component Number', fontsize=12)
    ax1.set_ylabel('Eigenvalue (Variance)', fontsize=12)
    ax1.set_title('Scree Plot', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(component_numbers[::2])  # Show every other component number
    
    # Cumulative variance explained
    ax2.plot(component_numbers, cumulative_percentage, 'ro-', linewidth=2, markersize=6)
    ax2.set_xlabel('Component Number', fontsize=12)
    ax2.set_ylabel('Cumulative Variance Explained (%)', fontsize=12)
    ax2.set_title('Cumulative Variance Explained', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.set_xticks(component_numbers[::2])
    ax2.set_ylim([0, 105])
    
    # Add horizontal line at 80% and 90%
    ax2.axhline(y=80, color='g', linestyle='--', alpha=0.5, label='80%')
    ax2.axhline(y=90, color='orange', linestyle='--', alpha=0.5, label='90%')
    ax2.legend()
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Variance explained plot saved to {output_path}")
    print(f"First component explains {cumulative_percentage[0]:.2f}% of variance")
    print(f"First 5 components explain {cumulative_percentage[4]:.2f}% of variance")


def main():
    """Main execution function."""
    # Define paths
    base_dir = Path(__file__).parent
    input_path = base_dir / 'data' / 'input' / 'taxi_wide_table.parquet'
    model_path = base_dir / 'pca_model.pkl'
    plot_path = base_dir / 'variance_explained.png'
    
    # Step 1: Load data
    X_dask, metadata_df = load_data(str(input_path))
    
    # Step 2: Handle missing values
    X_filled, mean_vector = handle_missing_values(X_dask)
    
    # Step 3: Compute covariance matrix
    cov_matrix = compute_covariance_matrix(X_filled, mean_vector)
    
    # Step 4: Perform PCA
    components, explained_variance = perform_pca(cov_matrix)
    
    # Step 5: Save model
    # mean_vector is already a numpy array from handle_missing_values
    save_model(components, explained_variance, mean_vector, str(model_path))
    
    # Step 6: Visualize variance explained
    visualize_variance_explained(explained_variance, str(plot_path))
    
    print("\nPCA fitting complete!")
    print(f"Model saved to: {model_path}")
    print(f"Visualization saved to: {plot_path}")


if __name__ == '__main__':
    main()
