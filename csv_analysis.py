import pandas as pd
import matplotlib.pyplot as plt

# Read the CSV file
file_path = '/Users/lunian/course/cloud_db/data/lineitem.1.csv'  # Replace with your CSV file path
data = pd.read_csv(file_path, delimiter='|')  # Use '|' as delimiter

# Calculate specific percentiles for L_EXTENDEDPRICE column
percentiles = [0.0001, 0.001, 0.01]
percentile_values = data['L_EXTENDEDPRICE'].quantile(percentiles)

# Plot histogram for L_EXTENDEDPRICE column
plt.figure(figsize=(10, 6))
plt.hist(data['L_EXTENDEDPRICE'], bins=50, alpha=0.7, color='green', edgecolor='black')

# Mark specific percentile values on the histogram
for p, pv in zip(percentiles, percentile_values):
    plt.axvline(pv, linestyle='--', color='red', label=f'{p*100}th percentile: {pv:.2f}')
    
plt.title('Histogram of L_EXTENDEDPRICE')
plt.xlabel('L_EXTENDEDPRICE')
plt.ylabel('Frequency')
plt.legend()
plt.show()

