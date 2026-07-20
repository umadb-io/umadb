import json
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Base directory for Criterion output
criterion_dir = Path("target/criterion/duplicate_check")

data = []

# Iterate through all method directories (e.g., brute_force, fxhashset)
if criterion_dir.exists():
    for method_dir in criterion_dir.iterdir():
        if not method_dir.is_dir():
            continue

        method_name = method_dir.name

        # Iterate through all size directories (e.g., 0, 1, 2, 4...)
        for size_dir in method_dir.iterdir():
            if not size_dir.is_dir():
                continue

            try:
                size_val = int(size_dir.name)
            except ValueError:
                # Skip folders that aren't integer sizes (like "report")
                continue

            # Path to the specific estimates file
            estimates_file = size_dir / "new" / "estimates.json"

            if estimates_file.exists():
                with open(estimates_file, "r") as f:
                    estimate_data = json.load(f)

                    # Criterion strictly stores these point estimates in nanoseconds
                    time_ns = estimate_data["mean"]["point_estimate"]

                    data.append({
                        "Method": method_name,
                        "Size": size_val,
                        "Time (ns)": time_ns
                    })
else:
    print(f"Error: Could not find directory {criterion_dir}. Are you in the project root?")

# --- Plotting ---
if data:
    df = pd.DataFrame(data)

    plt.figure(figsize=(10, 6))

    for method in df['Method'].unique():
        subset = df[df['Method'] == method].sort_values(by='Size')
        plt.plot(subset['Size'], subset['Time (ns)'], marker='o', label=method, linewidth=2)

    # Symlog for X-axis to handle the size '0', Log for Y-axis for scale differences
    plt.xscale('symlog', base=2)
    plt.yscale('log')

    plt.xlabel('Input Size (Items)')
    plt.ylabel('Execution Time (ns)')
    plt.title('Duplicate Check Performance: Brute Force vs. HashSets')
    plt.legend()
    plt.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()

    plt.show()
else:
    print("No data found to plot.")