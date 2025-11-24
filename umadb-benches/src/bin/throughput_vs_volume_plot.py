#!/usr/bin/env python3
"""
UmaDB Throughput Testing Script

This script:
1. Starts the UmaDB server in a background thread
2. Continuously appends batches of 10000 events
3. Every 5 seconds, measures throughput by timing single event appends
4. Logs position and throughput to umadb-throughput.log
5. Generates a plot of throughput vs position (UmaDB-throughput.png)
"""
import datetime
import gc
import signal
import subprocess
import threading
import time
from pathlib import Path

import numpy as np
from scipy.optimize import curve_fit
import matplotlib
# matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

from umadb import Client, Event


def human_readable_formatter(x, pos):
    """
    Format large numbers in human-readable format (e.g., 1M, 2.5K)
    
    Args:
        x: The tick value
        pos: The tick position (required by FuncFormatter but not used)
    
    Returns:
        Formatted string
    """
    if x >= 1e6:
        return f'{x/1e6:.0f}M'
    elif x >= 1e3:
        return f'{x/1e3:.0f}K'
    else:
        return f'{x:.0f}'


def inv_log_offset_model(x, c, d, e):
    """
    Inverse logarithmic offset model: y = c / (log(x) + d) + e
    
    This model represents asymptotic saturation behavior where throughput
    approaches a constant value (e) as volume increases.

    Args:
        x: Input values (volume/position)
        c: Numerator constant
        d: Logarithm offset constant
        e: Asymptotic saturation level

    Returns:
        y values based on inverse logarithmic offset model
    """
    return c / (np.log(x) + d) + e

def safe_fit_inv_log(x, y):
    # filter invalid x (<=0)
    mask = np.isfinite(x) & np.isfinite(y) & (x > 0)
    x = x[mask]
    y = y[mask]
    if x.size < 3:
        raise ValueError("Need at least 3 valid points to fit.")

    # things derived from the data
    min_log_x = np.log(x).min()
    max_log_x = np.log(x).max()

    # sensible initial guesses
    e0 = np.percentile(y, 5)        # asymptote guess: low percentile of measured throughput
    c0 = (np.max(y) - e0) * (np.log(np.median(x)) + 1.0)  # heuristic
    d0 = 1.0 - min_log_x           # shift so denominator is positive initially

    p0 = [c0, d0, e0]

    # bounds: keep c positive (throughput decreasing with x), and denominator safe:
    # Require log(x)+d >= eps for all x in fitted range -> d >= -min_log_x + eps
    eps = 1e-6
    d_lower = -min_log_x + eps

    lower = [0.0, d_lower, 0.0]  # don't allow negative c or e (throughput neg doesn't make sense)
    upper = [np.inf, np.inf, np.inf]

    try:
        popt, pcov = curve_fit(inv_log_offset_model, x, y, p0=p0, bounds=(lower, upper),
                               maxfev=20000)
    except RuntimeError:
        # fallback: try with looser initial guesses
        popt, pcov = curve_fit(inv_log_offset_model, x, y, p0=[c0*0.5, d0, e0*0.5],
                               bounds=(lower, upper), maxfev=40000)

    # compute fitted curve and diagnostics
    x_smooth = np.linspace(x.min(), x.max()*1.25, 500)
    y_smooth = inv_log_offset_model(x_smooth, *popt)

    # R^2
    y_pred = inv_log_offset_model(x, *popt)
    ss_res = np.sum((y - y_pred)**2)
    ss_tot = np.sum((y - np.mean(y))**2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else np.nan

    return {
        'popt': popt,
        'pcov': pcov,
        'x': x,
        'y': y,
        'x_smooth': x_smooth,
        'y_smooth': y_smooth,
        'r2': r2
    }

def plot_throughput(log_file='../umadb-throughput.log', output_file='../UmaDB-throughput.png'):
    """
    Read throughput log and generate a plot
    
    Args:
        log_file: Path to the log file containing position and throughput data
        output_file: Path to save the output PNG file
    """
    positions = []
    throughputs = []
    
    # Read the log file
    if Path(log_file).exists():
        with open(log_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split()
                    if len(parts) == 2:
                        try:
                            position = int(parts[0])
                            throughput = float(parts[1])
                            positions.append(position)
                            throughputs.append(throughput)
                        except ValueError:
                            continue
    else:
        raise Exception("File not found:", log_file)
    
    # Create the plot
    if positions and throughputs:
        plt.figure(figsize=(10, 6))
        
        # Plot original data points
        # plt.plot(positions, throughputs, 'b-', linewidth=2, marker='o', markersize=4, label='Actual Data')

        x_data = np.array(positions)
        y_data = np.array(throughputs)
        result = safe_fit_inv_log(x_data, y_data)
        c_fit, d_fit, e_fit = result['popt']
        # then plot:
        plt.scatter(result['x'], result['y'], label='data', s=6)
        plt.plot(result['x_smooth'], result['y_smooth'], 'r--', label=f'fit: {c_fit:.0f}/(ln(x){"+" if d_fit > 0 else "-"}{(d_fit if d_fit > 0 else -d_fit):.0f}) + {e_fit:.0f}')
        plt.legend()
        # # Fit inverse logarithmic offset curve
        # try:
        #     # Convert to numpy arrays
        #
        #     # Filter out zero or negative positions for log fitting
        #     valid_indices = x_data > 0
        #     x_valid = x_data[valid_indices]
        #     y_valid = y_data[valid_indices]
        #
        #     if len(x_valid) > 3:  # Need at least 4 points for meaningful fit with 3 parameters
        #         # Perform curve fitting
        #         popt, _ = curve_fit(inv_log_offset_model, x_valid, y_valid)
        #         c_fit, d_fit, e_fit = popt
        #
        #         # Generate smooth curve for plotting
        #         x_smooth = np.linspace(x_valid.min(), x_valid.max(), 500)
        #         y_smooth = inv_log_offset_model(x_smooth, c_fit, d_fit, e_fit)
        #
        #         # Plot fitted curve
        #         plt.plot(x_smooth, y_smooth, 'r--', linewidth=2,
        #                 label=f'Fit: y = {c_fit:.2f}/(ln(x) + {d_fit:.2f}) + {e_fit:.2f}')
        #
        # except Exception as e:
        #     print(f"Warning: Could not fit inverse logarithmic offset curve: {e}")
        
        # Format x-axis with human-readable labels
        # plt.xscale('log')
        ax = plt.gca()
        ax.xaxis.set_major_formatter(FuncFormatter(human_readable_formatter))
        
        plt.xlabel('Volume', fontsize=12)
        plt.ylabel('Throughput (events/s)', fontsize=12)
        plt.xlim(0)
        plt.ylim(0)
        plt.title('UmaDB Throughput vs Volume (One Client, One Event per Request)', fontsize=14)
        # plt.legend(loc='best', fontsize=10)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_file, dpi=150)
        plt.show()
        plt.close()
        # print(f"Plot saved to {output_file}")



if __name__ == "__main__":
    plot_throughput()
