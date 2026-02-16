"""
Tail Analysis for HW2 Part 2

Analyze the distribution of PCA coefficients (eigenvector loadings) to characterize tail behavior.
"""

import pickle
import json
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from pathlib import Path


def load_pca_model(model_path: str):
    """
    Load PCA model and extract all coefficient values.
    
    Args:
        model_path: Path to pca_model.pkl
        
    Returns:
        all_coefficients: Flattened array of all eigenvector coefficients
        components: Original components matrix
    """
    print(f"Loading PCA model from {model_path}...")
    
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    components = model['components']  # Shape: (24, 24)
    
    # Pool all loading values from all eigenvectors
    all_coefficients = components.flatten()
    
    print(f"Loaded {len(all_coefficients)} coefficients from {components.shape[0]} components")
    print(f"Coefficient statistics: mean={np.mean(all_coefficients):.4f}, std={np.std(all_coefficients):.4f}")
    
    return all_coefficients, components


def create_histogram(coefficients: np.ndarray, ax):
    """
    Create histogram of coefficient distribution with normal overlay.
    
    Args:
        coefficients: Array of all coefficient values
        ax: Matplotlib axes object
    """
    # Create histogram
    n, bins, patches = ax.hist(coefficients, bins=50, density=True, alpha=0.7, 
                                color='steelblue', edgecolor='black', linewidth=0.5)
    
    # Overlay normal distribution
    mean = np.mean(coefficients)
    std = np.std(coefficients)
    x = np.linspace(coefficients.min(), coefficients.max(), 100)
    normal_pdf = stats.norm.pdf(x, mean, std)
    ax.plot(x, normal_pdf, 'r-', linewidth=2, label=f'Normal(μ={mean:.3f}, σ={std:.3f})')
    
    ax.set_xlabel('Coefficient Value', fontsize=11)
    ax.set_ylabel('Density', fontsize=11)
    ax.set_title('Histogram of PCA Coefficients', fontsize=12, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)


def create_qq_plot(coefficients: np.ndarray, ax):
    """
    Create Q-Q plot comparing against standard normal.
    
    Args:
        coefficients: Array of all coefficient values
        ax: Matplotlib axes object
    """
    # Q-Q plot against standard normal N(0,1) using standardized data
    standardized = (coefficients - np.mean(coefficients)) / np.std(coefficients)
    (osm, osr), (slope, intercept, r) = stats.probplot(standardized, dist="norm")
    
    # Plot standard normal Q-Q
    ax.scatter(osm, osr, alpha=0.6, s=20, color='steelblue', label='Data', zorder=2)
    ax.plot(osm, slope * osm + intercept, 'r-', linewidth=2, label='Standard Normal N(0,1)', zorder=1)
    
    ax.set_xlabel('Theoretical Quantiles', fontsize=11)
    ax.set_ylabel('Sample Quantiles', fontsize=11)
    ax.set_title('Q-Q Plot (Normal)', fontsize=12, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.legend()


def create_survival_plot(coefficients: np.ndarray, ax):
    """
    Create log-log survival plot using absolute values of coefficients.
    
    Args:
        coefficients: Array of all coefficient values
        ax: Matplotlib axes object
        
    Returns:
        alpha: Estimated tail exponent (None if fit fails)
        r_squared: R² of power-law fit (None if fit fails)
        tail_threshold: Threshold used for tail fitting
    """
    # Use absolute values
    abs_coeffs = np.abs(coefficients)
    abs_coeffs_sorted = np.sort(abs_coeffs)[::-1]  # Sort descending (largest first)
    
    # Compute survival function: P(X > x) = 1 - CDF(x)
    # For empirical CDF: CDF(x) = (number of values <= x) / n
    # For sorted descending at index i: there are (n-i) values <= x_i
    # So CDF(x_i) = (n-i) / n, and Survival(x_i) = 1 - (n-i)/n = i/n
    # But we want P(X > x), not P(X >= x), so we use (i+1)/n to avoid 0
    n = len(abs_coeffs_sorted)
    # Use (i+1)/n to get non-zero survival values, or use 1 - empirical CDF
    # Alternative: use rank-based approach
    ranks = np.arange(1, n + 1)  # Rank from 1 to n (largest has rank 1)
    # P(X > x_i) = (rank - 1) / n, but we'll use rank / (n+1) for better estimation
    survival = ranks / (n + 1)  # P(X > x) using rank-based estimator
    
    # Convert to log-log scale
    log_x = np.log(abs_coeffs_sorted)
    log_survival = np.log(survival)
    
    # Remove any -inf or inf values
    valid_mask = np.isfinite(log_x) & np.isfinite(log_survival) & (log_survival > -np.inf)
    log_x_valid = log_x[valid_mask]
    log_survival_valid = log_survival[valid_mask]
    
    # Plot all points
    ax.scatter(log_x_valid, log_survival_valid, alpha=0.5, s=10, color='steelblue', label='Data')
    
    # Fit power-law to tail region (largest 20% of values)
    # Since abs_coeffs_sorted is descending, largest values are at the beginning
    tail_fraction = 0.2
    tail_end_idx = int(len(log_x_valid) * tail_fraction)
    tail_x = log_x_valid[:tail_end_idx]  # First 20% (largest values)
    tail_y = log_survival_valid[:tail_end_idx]
    
    if len(tail_x) > 2:
        # Fit linear regression: log(P(X > x)) = -alpha * log(x) + c
        slope, intercept, r_value, p_value, std_err = stats.linregress(tail_x, tail_y)
        alpha = -slope  # Power-law exponent
        r_squared = r_value ** 2
        
        # Plot fitted line
        x_fit = np.linspace(tail_x.min(), tail_x.max(), 100)
        y_fit = slope * x_fit + intercept
        ax.plot(x_fit, y_fit, 'r-', linewidth=2, 
                label=f'Power-law fit (α={alpha:.3f}, R²={r_squared:.3f})')
        
        # Highlight tail region
        ax.axvline(x=tail_x.max(), color='orange', linestyle='--', alpha=0.7, 
                   label=f'Tail region (largest {tail_fraction*100:.0f}%)')
        
        tail_threshold = np.exp(tail_x.max())  # Convert back to original scale (largest value in tail)
    else:
        alpha = None
        r_squared = None
        tail_threshold = None
    
    ax.set_xlabel('log(|Coefficient|)', fontsize=11)
    ax.set_ylabel('log(P(X > x))', fontsize=11)
    ax.set_title('Log-Log Survival Plot', fontsize=12, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    return alpha, r_squared, tail_threshold


def classify_tail(qq_deviation: float, survival_linearity: float, r_squared: float = None):
    """
    Classify tail behavior as light (Gaussian-like) or heavy (power-law).
    
    Args:
        qq_deviation: Measure of Q-Q plot deviation from normal (not used in simple version)
        survival_linearity: R² from power-law fit (higher = more linear = heavy tail)
        r_squared: R² of power-law fit
        
    Returns:
        classification: "light" or "heavy"
    """
    # Simple classification based on R² of power-law fit
    # High R² (> 0.9) suggests heavy tail (power-law)
    # Low R² (< 0.7) suggests light tail (Gaussian-like)
    if r_squared is None:
        return "light"  # Default to light if no fit
    
    if r_squared > 0.85:
        return "heavy"
    elif r_squared < 0.7:
        return "light"
    else:
        # Intermediate case - check other factors
        # For now, classify as heavy if R² is decent
        return "heavy" if r_squared > 0.75 else "light"


def create_visualizations(coefficients: np.ndarray, output_path: str):
    """
    Create 3-panel visualization: histogram, Q-Q plot, and log-log survival plot.
    
    Args:
        coefficients: Array of all coefficient values
        output_path: Path to save the figure
        
    Returns:
        alpha: Estimated tail exponent
        r_squared: R² of power-law fit
        tail_threshold: Threshold used for tail fitting
    """
    print("Creating visualizations...")
    
    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    
    # Panel 1: Histogram
    create_histogram(coefficients, axes[0])
    
    # Panel 2: Q-Q Plot
    create_qq_plot(coefficients, axes[1])
    
    # Panel 3: Log-Log Survival Plot
    alpha, r_squared, tail_threshold = create_survival_plot(coefficients, axes[2])
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"Visualization saved to {output_path}")
    
    return alpha, r_squared, tail_threshold


def save_report(coefficients: np.ndarray, tail_classification: str, 
                alpha: float, r_squared: float, tail_threshold: float, output_path: str):
    """
    Save tail analysis report as JSON.
    
    Args:
        coefficients: Array of all coefficient values
        tail_classification: "light" or "heavy"
        alpha: Tail exponent (or None)
        r_squared: R² of power-law fit (or None)
        tail_threshold: Threshold used for tail fitting (or None)
        output_path: Path to save the JSON file
    """
    print(f"Saving report to {output_path}...")
    
    report = {
        'tail_classification': tail_classification,
        'alpha': float(alpha) if alpha is not None else None,
        'r_squared': float(r_squared) if r_squared is not None else None,
        'mean': float(np.mean(coefficients)),
        'std': float(np.std(coefficients)),
        'n_coefficients': int(len(coefficients)),
        'tail_threshold': float(tail_threshold) if tail_threshold is not None else None
    }
    
    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Report saved successfully")
    print(f"Tail classification: {tail_classification}")
    if alpha is not None:
        print(f"Alpha (tail exponent): {alpha:.4f}")
        print(f"R²: {r_squared:.4f}")


def main():
    """Main execution function."""
    # Define paths
    base_dir = Path(__file__).parent
    model_path = base_dir / 'pca_model.pkl'
    plot_path = base_dir / 'coefficient_distribution.png'
    report_path = base_dir / 'tail_analysis_report.json'
    
    # Step 1: Load PCA model and extract coefficients
    all_coefficients, components = load_pca_model(str(model_path))
    
    # Step 2: Create visualizations
    alpha, r_squared, tail_threshold = create_visualizations(all_coefficients, str(plot_path))
    
    # Step 3: Classify tail behavior
    tail_classification = classify_tail(0, 0, r_squared)
    
    # Step 4: Save report
    save_report(all_coefficients, tail_classification, alpha, r_squared, 
                tail_threshold, str(report_path))
    
    print("\nTail analysis complete!")
    print(f"Visualization saved to: {plot_path}")
    print(f"Report saved to: {report_path}")


if __name__ == '__main__':
    main()
