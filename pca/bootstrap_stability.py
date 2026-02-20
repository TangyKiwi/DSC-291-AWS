"""
Bootstrap Stability Analysis for HW2 Part 4

Assess eigenvector stability under bootstrap resampling of rows.
"""

import pickle
import json
import numpy as np
import dask.array as da
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


def load_data_and_model(parquet_path: str, model_path: str, K: int = 3):
    """
    Load original data and PCA model, extract first K eigenvectors.
    
    Args:
        parquet_path: Path to taxi_wide_table.parquet
        model_path: Path to pca_model.pkl
        K: Number of eigenvectors to extract
        
    Returns:
        X: Feature matrix (n_samples, n_features)
        original_components: First K eigenvectors (K, n_features)
        mean: Mean vector used for centering
    """
    print(f"Loading PCA model from {model_path}...")
    
    # Load PCA model
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    components = model['components']  # Shape: (24, 24)
    mean = model['mean']  # Shape: (24,)
    
    # Extract first K eigenvectors
    original_components = components[:K, :]  # Shape: (K, 24)
    
    print(f"Loaded PCA model: {components.shape[0]} components")
    print(f"Extracted first {K} eigenvectors for stability analysis")
    
    # Load data
    print(f"Loading data from {parquet_path}...")
    df = dd.read_parquet(parquet_path, engine='pyarrow')
    
    # Extract hour columns
    hour_cols = [f'hour_{i}' for i in range(24)]
    X_df = df[hour_cols]
    X_pandas = X_df.compute()
    X = X_pandas.values  # Shape: (n_samples, 24)
    
    print(f"Loaded {X.shape[0]} samples, {X.shape[1]} features")
    
    return X, original_components, mean


def handle_missing_values_bootstrap(X: np.ndarray):
    """
    Replace missing values with column means (excluding missing values).
    
    Args:
        X: Data matrix (n_samples, n_features)
        
    Returns:
        X_filled: Data matrix with missing values replaced
        mean_vector: Column means
    """
    # Compute column means excluding missing values
    mean_vector = np.nanmean(X, axis=0)
    
    # Replace NaN with column means
    X_filled = X.copy()
    nan_mask = np.isnan(X_filled)
    X_filled = np.where(nan_mask, mean_vector, X_filled)
    
    return X_filled, mean_vector


def compute_covariance_matrix_bootstrap(X: np.ndarray, mean_vector: np.ndarray):
    """
    Compute covariance matrix for bootstrap sample.
    
    Args:
        X: Data matrix (n_samples, n_features)
        mean_vector: Column means
        
    Returns:
        cov_matrix: Covariance matrix (n_features, n_features)
    """
    # Center the data
    X_centered = X - mean_vector
    
    # Get number of samples
    n_samples = X.shape[0]
    
    # Compute covariance: C = (1/(n-1)) * X_centered.T @ X_centered
    cov_matrix = (X_centered.T @ X_centered) / (n_samples - 1)
    
    return cov_matrix


def perform_pca_bootstrap(cov_matrix: np.ndarray, K: int = 3):
    """
    Perform PCA via eigenvalue decomposition and extract first K eigenvectors.
    
    Args:
        cov_matrix: Covariance matrix (n_features, n_features)
        K: Number of eigenvectors to extract
        
    Returns:
        components: First K eigenvectors (K, n_features)
    """
    # Perform eigenvalue decomposition
    eigenvalues, eigenvectors = np.linalg.eigh(cov_matrix)
    
    # Sort by eigenvalues in descending order
    idx = np.argsort(eigenvalues)[::-1]
    eigenvectors_sorted = eigenvectors[:, idx]
    
    # Normalize eigenvectors
    norms = np.linalg.norm(eigenvectors_sorted, axis=0)
    eigenvectors_normalized = eigenvectors_sorted / norms
    
    # Extract first K and transpose (rows = components, columns = features)
    components = eigenvectors_normalized[:, :K].T  # Shape: (K, n_features)
    
    return components


def bootstrap_pca(X: np.ndarray, B: int = 100, K: int = 3):
    """
    Perform bootstrap resampling and fit PCA for each sample.
    
    Args:
        X: Original data matrix (n_samples, n_features)
        B: Number of bootstrap iterations
        K: Number of eigenvectors to extract
        
    Returns:
        bootstrap_components: List of B arrays, each of shape (K, n_features)
    """
    print(f"Performing {B} bootstrap iterations...")
    
    n_samples = X.shape[0]
    bootstrap_components = []
    
    for b in range(B):
        # Resample rows with replacement
        bootstrap_indices = np.random.choice(n_samples, size=n_samples, replace=True)
        X_boot = X[bootstrap_indices]
        
        # Handle missing values
        X_filled, mean_vector = handle_missing_values_bootstrap(X_boot)
        
        # Compute covariance matrix
        cov_matrix = compute_covariance_matrix_bootstrap(X_filled, mean_vector)
        
        # Fit PCA and extract first K eigenvectors
        components = perform_pca_bootstrap(cov_matrix, K)
        
        bootstrap_components.append(components)
        
        # Print progress every 20 iterations
        if (b + 1) % 20 == 0 or (b + 1) == B:
            print(f"Completed {b + 1}/{B} bootstrap iterations")
    
    print(f"Completed {B} bootstrap iterations")
    
    return bootstrap_components


def compute_subspace_affinity(U_orig: np.ndarray, U_boot: np.ndarray):
    """
    Compute subspace affinity between original and bootstrap subspaces.
    
    Args:
        U_orig: Original eigenvectors (K, n_features) - rows are components
        U_boot: Bootstrap eigenvectors (K, n_features) - rows are components
        
    Returns:
        affinity: Subspace affinity value
    """
    # Convert to column format (n_features, K) for subspace representation
    U_orig_cols = U_orig.T  # (n_features, K)
    U_boot_cols = U_boot.T  # (n_features, K)
    
    # Subspace affinity = trace(U_orig^T @ U_boot @ U_boot^T @ U_orig) / K
    affinity = np.trace(U_orig_cols.T @ U_boot_cols @ U_boot_cols.T @ U_orig_cols) / U_orig.shape[0]
    
    return affinity


def compute_procrustes_distance(U_orig: np.ndarray, U_boot: np.ndarray):
    """
    Compute Procrustes distance between original and bootstrap eigenvectors.
    
    Args:
        U_orig: Original eigenvectors (K, n_features) - rows are components
        U_boot: Bootstrap eigenvectors (K, n_features) - rows are components
        
    Returns:
        procrustes_dist: Procrustes distance
    """
    # Convert to column format (n_features, K)
    U_orig_cols = U_orig.T  # (n_features, K)
    U_boot_cols = U_boot.T  # (n_features, K)
    
    # Find optimal rotation: min ||U_orig - U_boot @ R||_F
    # Using SVD: U_orig^T @ U_boot = U @ S @ V^T, then R = U @ V^T
    U, S, Vt = np.linalg.svd(U_orig_cols.T @ U_boot_cols)
    R = U @ Vt  # Optimal rotation matrix (K, K)
    
    # Procrustes distance
    procrustes_dist = np.linalg.norm(U_orig_cols - U_boot_cols @ R, 'fro')
    
    return procrustes_dist


def compute_component_correlations(U_orig: np.ndarray, U_boot: np.ndarray):
    """
    Compute component-wise correlations between original and bootstrap eigenvectors.
    
    Args:
        U_orig: Original eigenvectors (K, n_features) - rows are components
        U_boot: Bootstrap eigenvectors (K, n_features) - rows are components
        
    Returns:
        correlations: Array of K correlation values (one per component)
    """
    K = U_orig.shape[0]
    correlations = np.zeros(K)
    
    for k in range(K):
        orig_k = U_orig[k, :]
        boot_k = U_boot[k, :]
        
        # Compute correlation
        corr = np.corrcoef(orig_k, boot_k)[0, 1]
        
        # Handle sign ambiguity: use absolute correlation
        # (eigenvectors can be flipped)
        correlations[k] = abs(corr)
    
    return correlations


def compute_stability_metrics(original_components: np.ndarray, bootstrap_components: list):
    """
    Compute all stability metrics for bootstrap analysis.
    
    Args:
        original_components: Original eigenvectors (K, n_features)
        bootstrap_components: List of B bootstrap eigenvectors, each (K, n_features)
        
    Returns:
        metrics: Dictionary with all computed metrics
    """
    print("Computing stability metrics...")
    
    B = len(bootstrap_components)
    K = original_components.shape[0]
    
    # Initialize arrays
    subspace_affinities = np.zeros(B)
    procrustes_distances = np.zeros(B)
    component_correlations = np.zeros((B, K))
    
    # Compute metrics for each bootstrap
    for b in range(B):
        U_boot = bootstrap_components[b]
        
        # Subspace affinity
        subspace_affinities[b] = compute_subspace_affinity(original_components, U_boot)
        
        # Procrustes distance
        procrustes_distances[b] = compute_procrustes_distance(original_components, U_boot)
        
        # Component-wise correlations
        component_correlations[b, :] = compute_component_correlations(original_components, U_boot)
        
        # Print progress every 20 iterations
        if (b + 1) % 20 == 0 or (b + 1) == B:
            print(f"Computed metrics for {b + 1}/{B} bootstrap samples")
    
    # Aggregate statistics
    metrics = {
        'n_bootstrap': B,
        'n_components': K,
        'subspace_affinity': {
            'mean': float(np.mean(subspace_affinities)),
            'std': float(np.std(subspace_affinities)),
            'min': float(np.min(subspace_affinities)),
            'max': float(np.max(subspace_affinities))
        },
        'procrustes_distance': {
            'mean': float(np.mean(procrustes_distances)),
            'std': float(np.std(procrustes_distances)),
            'min': float(np.min(procrustes_distances)),
            'max': float(np.max(procrustes_distances))
        },
        'component_correlations': {
            'mean': [float(np.mean(component_correlations[:, k])) for k in range(K)],
            'std': [float(np.std(component_correlations[:, k])) for k in range(K)],
            'median': [float(np.median(component_correlations[:, k])) for k in range(K)]
        }
    }
    
    print("Stability metrics computed")
    print(f"\nCorrelation statistics:")
    for k in range(K):
        corr_mean = np.mean(component_correlations[:, k])
        corr_std = np.std(component_correlations[:, k])
        corr_min = np.min(component_correlations[:, k])
        corr_max = np.max(component_correlations[:, k])
        print(f"  PC{k+1}: mean={corr_mean:.6f}, std={corr_std:.6f}, range=[{corr_min:.6f}, {corr_max:.6f}]")
    
    return metrics, subspace_affinities, procrustes_distances, component_correlations


def visualize_stability(original_components: np.ndarray, bootstrap_components: list,
                       component_correlations: np.ndarray, output_path: str):
    """
    Create visualizations: bootstrap PC1 band and boxplot of correlations.
    
    Args:
        original_components: Original eigenvectors (K, n_features)
        bootstrap_components: List of B bootstrap eigenvectors
        component_correlations: Array of shape (B, K) with correlations
        output_path: Path to save the figure
    """
    print("Creating visualizations...")
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    # Panel 1: Bootstrap PC1 Band
    K = original_components.shape[0]
    n_features = original_components.shape[1]
    hours = np.arange(n_features)
    
    # Extract PC1 from all bootstrap samples
    pc1_bootstrap = np.array([comp[0, :] for comp in bootstrap_components])  # (B, n_features)
    pc1_original = original_components[0, :]  # (n_features,)
    
    # Compute statistics
    pc1_mean = np.mean(pc1_bootstrap, axis=0)
    pc1_std = np.std(pc1_bootstrap, axis=0)
    pc1_lower = np.percentile(pc1_bootstrap, 5, axis=0)
    pc1_upper = np.percentile(pc1_bootstrap, 95, axis=0)
    
    # Plot original PC1
    ax1.plot(hours, pc1_original, 'r-', linewidth=2.5, label='Original PC1', zorder=3)
    
    # Plot mean of bootstrap PC1s
    ax1.plot(hours, pc1_mean, 'b--', linewidth=2, label='Bootstrap Mean', zorder=2)
    
    # Plot confidence band (5th-95th percentile)
    ax1.fill_between(hours, pc1_lower, pc1_upper, alpha=0.3, color='blue', 
                     label='95% Confidence Band', zorder=1)
    
    # Also show mean ± std
    ax1.fill_between(hours, pc1_mean - pc1_std, pc1_mean + pc1_std, 
                     alpha=0.2, color='green', label='Mean ± 1 Std', zorder=1)
    
    ax1.set_xlabel('Hour (0-23)', fontsize=12)
    ax1.set_ylabel('Eigenvector Coefficient', fontsize=12)
    ax1.set_title('Bootstrap PC1 Band', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xticks(hours[::2])  # Show every other hour
    
    # Panel 2: Boxplot of Correlations
    # Prepare data for boxplot
    boxplot_data = [component_correlations[:, k] for k in range(K)]
    component_labels = [f'PC{k+1}' for k in range(K)]
    
    bp = ax2.boxplot(boxplot_data, tick_labels=component_labels, patch_artist=True)
    
    # Color the boxes
    colors = ['lightblue', 'lightgreen', 'lightcoral']
    for patch, color in zip(bp['boxes'], colors[:K]):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    
    # Add mean markers
    means = [np.mean(component_correlations[:, k]) for k in range(K)]
    ax2.scatter(range(1, K+1), means, color='red', marker='D', s=100, 
               zorder=3, label='Mean', edgecolors='black', linewidths=1)
    
    ax2.set_ylabel('Correlation', fontsize=12)
    ax2.set_title('Component-wise Correlations', fontsize=14, fontweight='bold')
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.set_ylim([0, 1.1])
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Visualization saved to {output_path}")


def save_report(metrics: dict, output_path: str):
    """
    Save bootstrap stability report as JSON.
    
    Args:
        metrics: Dictionary with stability metrics
        output_path: Path to save JSON file
    """
    print(f"Saving report to {output_path}...")
    
    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"Report saved successfully")


def main():
    """Main execution function."""
    # Parameters
    B = 100  # Number of bootstrap iterations
    K = 5    # Number of eigenvectors to analyze
    
    # Define paths
    base_dir = Path(__file__).parent
    model_path = base_dir / 'pca_model.pkl'
    parquet_path = base_dir / 'data' / 'input' / 'taxi_wide_table.parquet'
    report_path = base_dir / 'bootstrap_stability_report.json'
    plot_path = base_dir / 'eigenvector_stability.png'
    
    # Step 1: Load data and original PCA model
    X, original_components, mean = load_data_and_model(
        str(parquet_path), str(model_path), K
    )
    
    # Step 2: Perform bootstrap resampling and fit PCA
    bootstrap_components = bootstrap_pca(X, B=B, K=K)
    
    # Step 3: Compute stability metrics
    metrics, subspace_affinities, procrustes_distances, component_correlations = \
        compute_stability_metrics(original_components, bootstrap_components)
    
    # Step 4: Create visualizations
    visualize_stability(original_components, bootstrap_components, 
                       component_correlations, str(plot_path))
    
    # Step 5: Save report
    save_report(metrics, str(report_path))
    
    print("\nBootstrap stability analysis complete!")
    print(f"Report saved to: {report_path}")
    print(f"Visualization saved to: {plot_path}")
    print(f"\nSummary:")
    print(f"  Subspace Affinity: {metrics['subspace_affinity']['mean']:.4f} ± {metrics['subspace_affinity']['std']:.4f}")
    print(f"  Procrustes Distance: {metrics['procrustes_distance']['mean']:.4f} ± {metrics['procrustes_distance']['std']:.4f}")
    for k in range(K):
        print(f"  PC{k+1} Correlation: {metrics['component_correlations']['mean'][k]:.4f} ± {metrics['component_correlations']['std'][k]:.4f}")


if __name__ == '__main__':
    main()
