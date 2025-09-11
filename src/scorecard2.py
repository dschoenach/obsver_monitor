import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from sklearn.preprocessing import minmax_scale

# --- 1. Load your data from the CSV file ---
# Make sure 'monitor_surface_zscore_data.csv' is in the same directory
df = pd.read_csv('monitor_surface_zscore_data.csv')


# --- 2. Pre-process the data for plotting ---

# Category 1: Positive or Negative direction
df['direction'] = np.where(df['rmse_diff'] > 0, 'Positive', 'Negative')

# Category 2: Is it significant?
df['is_significant'] = df['significance'] > 0.95

# Property 1: Alpha (transparency)
df['plot_alpha'] = 0.0
significant_mask = df['is_significant']

# --- MODIFIED SECTION: Scale alpha per variable ---
# Loop through each variable to scale alpha independently
for var in df['obstypevar'].unique():
    # Create a mask for the current variable that is also significant
    var_significant_mask = (df['obstypevar'] == var) & significant_mask

    # Check if there are any significant values for this variable
    if var_significant_mask.any():
        # Get the absolute rmse_diff values for scaling for the current variable
        values_to_scale = df.loc[var_significant_mask, 'rmse_diff'].abs()
        
        # Apply minmax_scale and assign back to the DataFrame
        df.loc[var_significant_mask, 'plot_alpha'] = minmax_scale(
            values_to_scale, feature_range=(0.5, 1.0)
        )

# Assign fixed, low alpha for all non-significant values
df.loc[~significant_mask, 'plot_alpha'] = 0.25

# Property 2: Border color
df['border_color'] = np.where(df['is_significant'], 'black', 'none')

# --- 3. Set up the plot ---
fig, ax = plt.subplots(figsize=(16, 9))

# Map variable names to y-axis coordinates for plotting
variables = sorted(df['obstypevar'].unique())
y_coords = {var: i for i, var in enumerate(variables)}

# --- 4. Loop through the data and draw each tile ---
for _, row in df.iterrows():
    # Get the correct color based on the 'direction'
    face_color = 'steelblue' if row['direction'] == 'Positive' else '#b2182b'

    # Define width and height. Non-significant tiles are smaller.
    base_height = 0.9  # Less than 1 to create vertical gaps
    if row['is_significant']:
        rect_width = 1.0
        rect_height = base_height
    else:
        rect_width = 0.8  # Make non-significant tiles smaller
        rect_height = 0.8 * base_height

    # Calculate position to keep the tile centered
    x_pos = row['lead_time'] - rect_width / 2
    y_pos = y_coords[row['obstypevar']] - rect_height / 2

    # Create the rectangle patch for the tile
    rect = patches.Rectangle(
        (x_pos, y_pos),
        width=rect_width,
        height=rect_height,
        facecolor=face_color,
        alpha=row['plot_alpha'],
        edgecolor=row['border_color'],
        linewidth=1.5
    )
    ax.add_patch(rect)

# --- 5. Finalize and style the plot ---

# MODIFIED: Set the aspect ratio to 2 for a 2:1 height-to-width tile ratio
ax.set_aspect(2)

# Set axes limits to provide some padding
ax.set_xlim(df['lead_time'].min() - 1, df['lead_time'].max() + 1)
ax.set_ylim(-1, len(variables))

# Set the ticks to correspond to our variables and lead times
ax.set_yticks(list(y_coords.values()))
ax.set_yticklabels(list(y_coords.keys()))
ax.set_xticks(sorted(df['lead_time'].unique()))

# Labels and Title
ax.set_xlabel('Lead Time (hours/days)', fontsize=12)
ax.set_ylabel('Variable', fontsize=12)
ax.set_title('Performance Scorecard', fontsize=16, pad=20)

# Replicate the minimal theme
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.spines['left'].set_visible(True)
ax.spines['bottom'].set_visible(True)
ax.tick_params(axis='both', which='both', length=0)
ax.grid(False)

# --- 6. Create a custom legend ---
legend_elements = [
    patches.Patch(facecolor='steelblue', edgecolor='none', alpha=0.2, label='Positive (Not Significant)'),
    patches.Patch(facecolor='#b2182b', edgecolor='none', alpha=0.2, label='Negative (Not Significant)'),
    patches.Patch(facecolor='steelblue', edgecolor='black', alpha=0.8, label='Positive (Significant)', linewidth=1.5),
    patches.Patch(facecolor='#b2182b', edgecolor='black', alpha=0.8, label='Negative (Significant)', linewidth=1.5)
]
ax.legend(handles=legend_elements, loc='center left', bbox_to_anchor=(1, 0.5), title="Legend")

plt.tight_layout()
plt.show()
