import pandas as pd

# Read in Scooter CSV
df = pd.read_csv('C:/Users/SidneyHoch/Downloads/scooter.csv')

### Basic Discovery & Search ###

# Print the columns
print("Columns: ", df.columns)

# Print the Types
print("Data Types: ", df.dtypes)

# Print the first 5 rows
print("First 5 Rows: ", df.head())

# Print single field
print(df['DURATION'])

# Print multiple fields
print(df[['trip_id', 'DURATION', 'start_location_name']])

# Sample 5 rows to print
print(df.sample(5))

# Slice the first 10 rows
print(df[:10])

# Pick row using index
print(df.loc[5])

# Pick field using row index
print(df.at[5, 'DURATION'])

# Pick data using a codition
user = df[df['user_id'] == 8417864]
print(user)

# Combining Conditional Statements
condOne = df['user_id'] == 8417864
condTwo = df['trip_ledger_id'] == 1488838
print(df[condOne & condTwo])

### Analyzing the Data ###

# Five-Number Summary of the Data
print(df.describe())

# Summary on single column
print(df['start_location_name'].describe())

# Counts of data
print(df['DURATION'].value_counts())

# Frequency as a percentage
print(df['DURATION'].value_counts(normalize=True))

# Drop null values from count
print(df['end_location_name'].value_counts(dropna=False))

# Find amount of null values in dataframe
print(df.isnull().sum())

# Using bins to categorize numerical data
print(df['trip_id'].value_counts(bins=10))

### Handling Common Issues in Pandas ###

# Dropping unwanted columns, Axis is for columns and inplace is to save the changes to the dataframe
df = df.drop(['region_id'], axis=1, inplace=True)

# Dropping unwanted rows
df = df.drop(index=[34225], axis=0, inplace=True)

# Drop null values
df = df.dropna(subset=['start_location_name'], inplace=True)

# axis specifies rows or columns with indexes or columns (0 or 1). It defaults  to rows.
# how specifies whether to drop rows or columns if all the values are null or if any value is null (all or any). It defaults to any.
# thresh allows more control than allowing you to specify an integer value of how many nulls must be present.
# subset allows you to specify a list of rows or columns to search.
# inplace allows you to modify the existing DataFrame. It defaults to False.

# Fill null values for location start
df['start_location_name'].fillna('Unknown', inplace=True)

# Filling multiple columns with different null values
startstop=df[(df['start_location_name'].isnull())&(df['end_location_name'].isnull())]
value={'start_location_name':'Start St.','end_location_name':'Stop St.'}
startstop = startstop.fillna(value=value)
startstop = startstop[['start_location_name','end_location_name']]

# Create filter for month of may to use in drop method
may = df[(df['month'] == 'May')]

# Drop all rows for month of May
df = df.drop(may.index, inplace=True)

# Check to see if drop worked
print(df['month'].value_counts())


### Creating and Modifying Columns ###

# Making column names all lower case, can also use capitalize, title, or upper
df.columns=[x.lower() for x in df.columns] 
print(df.columns)

# Changing the column names
df.rename(columns={'DURATION':'duration'},inplace=True)

# Changing the column values
df['month'] = df['month'].str.upper()

# Creating a new column based on value
df = df.loc[df['month']=='JUNE', 'june_column'] = 'JUNE'

# Using str.split() to create new columns in new df, using expand=True 
DFdateTime = df['started_at'].str.split(expand=True)
# Add new columns to original df
df['Date'] = DFdateTime[0]
df['Time'] = DFdateTime[1]

# Changing the data type of a column
df['started_at'] = pd.to_datetime(df['started_at'])

# Filtering usimg datetime
print(df[df['started_at'] > '2019-05-23'])

### Enriching Data from multiple Sources ###

def enrichment():
    # Read in the CSV
    newdf = pd.read_csv('C:/Users/SidneyHoch/Downloads/scooter.csv')
    
    # Create a new dataframe with the top 5 value counts of the start location name
    newdf = pd.DataFrame(newdf['start_location_name'].value_counts().head())

    # reset index to make the start location name a column
    newdf.reset_index(inplace=True)

    # rename columns
    newdf.columns = ['address', 'count']

    # Print the new dataframe
    print(newdf)

    # We only need street address and zip code to geocode, the row index 1 is an instersection: Central @ Tingley which we will split
    n = newdf['address'].str.split(pat=',', n=1, expand=True) # Split the address

    n['street'] = n[0] # Street 
    n['address'] = n[1] # Address
    replaced = n['street'].str.replace('@', 'and') # Replace @ with and for geocoding

    # Add into dataframe
    newdf['street'] = replaced
    newdf['address'] = n['address']

    # Change column order
    newdf = newdf[['street','address','count']]
    print(newdf)

    # Read in the geocoded data
    geocode = pd.read_csv('C:/Users/SidneyHoch/Downloads/geocodedstreet.csv')

    # Join the dataframes
    joined = pd.merge(newdf, geocode, on='street', how='left')
