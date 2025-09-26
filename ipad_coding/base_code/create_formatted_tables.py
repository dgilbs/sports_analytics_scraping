import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.colors import LinearSegmentedColormap
import numpy as np

# Your data
data = {
    'Player': ['Alyssa Malonson', 'Phoebe McClernon', 'Hailie Mace', 'Taylor Flint', 
               'Miyabi Moriya', 'Riley Jackson', 'Delphine Cascarino', 'Croix Bethune', 
               'Jess Fishlock', "Denise O'Sullivan"],
    'Position': ['LB', 'CB', 'RB', 'CM', 'LB', 'CM', 'RW', 'AM', 'AM', 'CM'],
    'Team': ['Bay FC', 'Seattle Reign', 'NJ/NY Gotham FC', 'Racing Louisville', 
             'Angel City FC', 'North Carolina Courage', 'San Diego Wave', 
             'Orlando Pride', 'Seattle Reign', 'North Carolina Courage'],
    'Opponent': ['Houston Dash', 'Orlando Pride', 'Chicago Red Stars', 'Angel City FC', 
                 'Racing Louisville', 'San Diego Wave', 'North Carolina Courage', 
                 'Seattle Reign', 'Orlando Pride', 'San Diego Wave'],
    'Minutes': [45, 90, 90, 90, 89, 90, 90, 65, 30, 90],
    'Tackling Score': [9.60, 5.60, 4.27, 4.27, 3.94, 3.90, 3.90, 3.32, 3.30, 3.15]
}

df = pd.DataFrame(data)

# METHOD 1: Professional matplotlib table
def create_professional_table(df, title="NWSL Tackling Leaders"):
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('tight')
    ax.axis('off')
    
    # Create the table
    table = ax.table(cellText=df.values,
                     colLabels=df.columns,
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])
    
    # Style the table
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)
    
    # Header styling
    for i in range(len(df.columns)):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(weight='bold', color='white')
        table[(0, i)].set_height(0.1)
    
    # Alternating row colors
    for i in range(1, len(df) + 1):
        for j in range(len(df.columns)):
            if i % 2 == 0:
                table[(i, j)].set_facecolor('#F2F2F2')
            else:
                table[(i, j)].set_facecolor('white')
            table[(i, j)].set_height(0.08)
    
    # Highlight top 3 tackling scores
    for i in range(1, 4):  # Top 3 rows
        table[(i, -1)].set_facecolor('#FFE6CC')  # Light orange for tackling score
        table[(i, -1)].set_text_props(weight='bold')
    
    # Add title
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    
    # Save the figure
    plt.savefig('nwsl_tackling_table.png', dpi=300, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    plt.show()

# METHOD 2: Seaborn-styled table with color gradient
def create_gradient_table(df, title="NWSL Tackling Leaders"):
    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('off')
    
    # Create color gradient for tackling scores
    norm = plt.Normalize(df['Tackling Score'].min(), df['Tackling Score'].max())
    colors = plt.cm.RdYlGn(norm(df['Tackling Score']))
    
    # Create table with colors
    table_data = []
    cell_colors = []
    
    # Add header
    table_data.append(list(df.columns))
    header_colors = ['#2C3E50'] * len(df.columns)
    cell_colors.append(header_colors)
    
    # Add data rows
    for idx, row in df.iterrows():
        table_data.append([str(val) for val in row])
        row_colors = ['white'] * (len(df.columns) - 1) + [colors[idx]]
        cell_colors.append(row_colors)
    
    # Create table
    table = ax.table(cellText=table_data[1:],  # Skip header in cellText
                     colLabels=table_data[0],   # Use first row as header
                     cellColours=cell_colors[1:],  # Skip header colors
                     cellLoc='center',
                     loc='center',
                     bbox=[0, 0, 1, 1])
    
    # Style table
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2.2)
    
    # Style headers
    for i in range(len(df.columns)):
        table[(0, i)].set_facecolor('#2C3E50')
        table[(0, i)].set_text_props(weight='bold', color='white')
        table[(0, i)].set_height(0.12)
    
    # Style data cells
    for i in range(1, len(df) + 1):
        for j in range(len(df.columns)):
            table[(i, j)].set_height(0.1)
            if j == len(df.columns) - 1:  # Tackling score column
                table[(i, j)].set_text_props(weight='bold')
    
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.savefig('nwsl_tackling_gradient_table.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()

# METHOD 3: Pandas styling (saves as image via matplotlib)
def create_styled_pandas_table(df, title="NWSL Tackling Leaders"):
    # Style the dataframe
    styled_df = df.style.background_gradient(subset=['Tackling Score'], 
                                           cmap='RdYlGn', 
                                           low=0.3, 
                                           high=0.9)
    
    # Additional styling
    styled_df = styled_df.set_table_styles([
        {'selector': 'th', 'props': [('background-color', '#4472C4'),
                                     ('color', 'white'),
                                     ('font-weight', 'bold'),
                                     ('text-align', 'center')]},
        {'selector': 'td', 'props': [('text-align', 'center'),
                                     ('padding', '8px')]},
        {'selector': 'table', 'props': [('border-collapse', 'collapse'),
                                        ('margin', '25px 0'),
                                        ('font-size', '12px')]}
    ])
    
    # Convert to matplotlib figure
    fig, ax = plt.subplots(figsize=(14, 8))
    ax.axis('off')
    
    # This requires additional setup - alternative approach below
    
# METHOD 4: Simple but effective approach
def create_clean_table(df, title="NWSL Tackling Leaders"):
    # Set up the figure
    fig, ax = plt.subplots(figsize=(12, 8))
    ax.axis('off')
    
    # Create table
    table = ax.table(cellText=df.values,
                     colLabels=df.columns,
                     cellLoc='center',
                     loc='center')
    
    # Styling
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.8)
    
    # Color coding
    header_color = '#40466e'
    row_colors = ['#f1f1f2', 'w']
    
    # Apply colors
    for i, key in enumerate(table.get_celld().keys()):
        cell = table.get_celld()[key]
        if key[0] == 0:  # Header row
            cell.set_facecolor(header_color)
            cell.set_text_props(weight='bold', color='white')
        else:
            cell.set_facecolor(row_colors[key[0] % 2])
            # Highlight top 3 tackling scores
            if key[0] <= 3 and key[1] == len(df.columns) - 1:
                cell.set_facecolor('#ffe6e6')
                cell.set_text_props(weight='bold')
    
    # Add title
    plt.title(title, fontsize=16, fontweight='bold', pad=20, color='#40466e')
    
    # Add rank numbers
    for i in range(len(df)):
        ax.text(-0.15, 0.45 - i * 0.08, f"{i+1}.", 
                transform=ax.transAxes, fontsize=12, fontweight='bold')
    
    # Save
    plt.tight_layout()
    plt.savefig('nwsl_tackling_clean_table.png', dpi=300, bbox_inches='tight',
                facecolor='white', edgecolor='none')
    plt.show()

# # Run the functions
# if __name__ == "__main__":
#     # Create all table versions
#     create_professional_table(df)
#     create_gradient_table(df)
#     create_clean_table(df)
    
#     print("All tables saved as PNG files!")
#     print("Files created:")
#     print("- nwsl_tackling_table.png")
#     print("- nwsl_tackling_gradient_table.png") 
#     print("- nwsl_tackling_clean_table.png")