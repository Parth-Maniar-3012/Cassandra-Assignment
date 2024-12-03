
# Load the dataset
url = "sales_100.csv"
df = pd.read_csv(url)
# print(df.head())

# Create Bronze Table
try:
    session.execute("""
    CREATE TABLE IF NOT EXISTS medallionarchitecture.bronze_sales (
        id UUID PRIMARY KEY,
        Region text,
        Country text,
        Item_Type text,
        Sales_Channel text,
        Order_Priority text,
        Order_Date text,
        Order_ID int,
        Units_Sold int,
        Unit_Price float,
        Unit_Cost float,
        Total_Revenue float,
        Total_Cost float,
        Total_Profit float
    );
    """)
    print("Table 'bronze_sales' created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")
    exit()

# Insert raw data into the Bronze table
try:
    insert_query = session.prepare("""
        INSERT INTO medallionarchitecture.bronze_sales (id, Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for _, row in df.iterrows():
        session.execute(insert_query, (
            uuid.uuid4(), row['Region'], row['Country'], row['Item Type'], row['Sales Channel'],
            row['Order Priority'], row['Order Date'], row['Order ID'], row['UnitsSold'],
            row['UnitPrice'], row['UnitCost'], row['TotalRevenue'], row['TotalCost'],
            row['TotalProfit']
        ))

    print("Bronze table populated with raw data.")
except Exception as e:
    print(f"Error inserting data: {e}")
    exit()

# Fetch and Display Data from Bronze Table
try:
    rows = session.execute('SELECT * FROM medallionarchitecture.bronze_sales LIMIT 10;')  # Fetch the first 10 rows
    # Convert rows to a Pandas DataFrame
    bronze_data = pd.DataFrame(list(rows), columns=[
        'id', 'Region', 'Country', 'Item_Type', 'Sales_Channel', 'Order_Priority', 'Order_Date',
        'Order_ID', 'Units_Sold', 'Unit_Price', 'Unit_Cost', 'Total_Revenue', 'Total_Cost', 'Total_Profit'
    ])

    # Display the data in a grid/tabular format
    print("Data from Bronze table (Terminal View):")
    print(tabulate(bronze_data, headers='keys', tablefmt='grid'))  # For terminal

    # For Jupyter Notebook or Colab
    # print("Data from Bronze table (Notebook View):")
    # display(bronze_data)  # For interactive table display in notebooks
except Exception as e:
    print(f"Error fetching data: {e}")


# Perform data cleaning
df_cleaned = df.drop_duplicates()
print("Cleaned Dataset preview:")
print(tabulate(df_cleaned.head(), headers='keys', tablefmt='grid'))  # Display cleaned dataset

# Create Silver Table
try:
    session.execute("""
    CREATE TABLE IF NOT EXISTS medallionarchitecture.silver_sales (
        id UUID PRIMARY KEY,
        Region text,
        Country text,
        Item_Type text,
        Sales_Channel text,
        Order_Priority text,
        Order_Date text,
        Order_ID int,
        Units_Sold int,
        Unit_Price float,
        Unit_Cost float,
        Total_Revenue float,
        Total_Cost float,
        Total_Profit float
    );
    """)
    print("Table 'silver_sales' created successfully.")
except Exception as e:
    print(f"Error creating Silver table: {e}")
    exit()

# Insert cleaned data into the Silver table
try:
    insert_query = session.prepare("""
        INSERT INTO medallionarchitecture.silver_sales (id, Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Units_Sold, Unit_Price, Unit_Cost, Total_Revenue, Total_Cost, Total_Profit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for _, row in df_cleaned.iterrows():
        session.execute(insert_query, (
            uuid.uuid4(), row['Region'], row['Country'], row['Item Type'], row['Sales Channel'],
            row['Order Priority'], row['Order Date'], row['Order ID'], row['UnitsSold'],
            row['UnitPrice'], row['UnitCost'], row['TotalRevenue'], row['TotalCost'],
            row['TotalProfit']
        ))

    print("Silver table populated with cleaned data.")
except Exception as e:
    print(f"Error inserting cleaned data: {e}")
    exit()

# Fetch and Display Data from Silver Table
try:
    rows = session.execute('SELECT * FROM medallionarchitecture.silver_sales LIMIT 10;')  # Fetch the first 10 rows
    # Convert rows to a Pandas DataFrame
    silver_data = pd.DataFrame(list(rows), columns=[
        'id', 'Region', 'Country', 'Item_Type', 'Sales_Channel', 'Order_Priority', 'Order_Date',
        'Order_ID', 'Units_Sold', 'Unit_Price', 'Unit_Cost', 'Total_Revenue', 'Total_Cost', 'Total_Profit'
    ])

    # Display the data in a grid/tabular format
    print("Data from Silver table (Terminal View):")
    print(tabulate(silver_data, headers='keys', tablefmt='grid'))  # For terminal

    # For Jupyter Notebook or Colab
    # print("Data from Silver table (Notebook View):")
    # display(silver_data)  # For interactive table display in notebooks
except Exception as e:
    print(f"Error fetching Silver table data: {e}")


# Perform aggregation: Group by Region
try:
    region_df = df_cleaned.groupby('Region').sum()[['TotalRevenue', 'TotalProfit']].reset_index()
    # print("Aggregated Data by Region:")
    # print(tabulate(region_df, headers='keys', tablefmt='grid'))  # Display aggregated data
except Exception as e:
    print(f"Error during aggregation: {e}")
    exit()

# Create Gold Table (Aggregated by Region)
try:
    session.execute("""
    CREATE TABLE IF NOT EXISTS medallionarchitecture.gold_sales_region (
        Region text PRIMARY KEY,
        Total_Revenue float,
        Total_Profit float
    );
    """)
    print("Table 'gold_sales_region' created successfully.")
except Exception as e:
    print(f"Error creating Gold table: {e}")
    exit()

# Insert aggregated data into the Gold table
try:
    insert_query = session.prepare("""
        INSERT INTO medallionarchitecture.gold_sales_region (Region, Total_Revenue, Total_Profit) VALUES (?, ?, ?)
    """)

    for _, row in region_df.iterrows():
        session.execute(insert_query, (row['Region'], row['TotalRevenue'], row['TotalProfit']))

    print("Gold table populated with regional aggregates.")
except Exception as e:
    print(f"Error inserting data into Gold table: {e}")
    exit()

# Fetch and Display Data from Gold Table
try:
    rows = session.execute('SELECT * FROM medallionarchitecture.gold_sales_region;')  # Fetch all rows
    # Convert rows to a Pandas DataFrame
    gold_region_data = pd.DataFrame(list(rows), columns=['Region', 'Total_Revenue', 'Total_Profit'])

    # Display the data in a grid/tabular format
    print("Data from Gold table (Terminal View):")
    print(tabulate(gold_region_data, headers='keys', tablefmt='grid'))  # For terminal

    # For Jupyter Notebook or Colab
    # print("Data from Gold table (Notebook View):")
    # display(gold_region_data)  # For interactive table display in notebooks
except Exception as e:
    print(f"Error fetching Gold table data: {e}")


# Check and process column names
try:
    # print("Column names in cleaned data:")
    # print(tabulate(pd.DataFrame({'Columns': df_cleaned.columns}), headers='keys', tablefmt='grid'))

    if 'Country' in df_cleaned.columns and 'TotalProfit' in df_cleaned.columns and 'TotalRevenue' in df_cleaned.columns:
        # Group by Country, aggregate total revenue and profit, and sort by TotalProfit
        top_countries_df = (
            df_cleaned.groupby('Country')
            .agg({'TotalProfit': 'sum', 'TotalRevenue': 'sum'})
            .sort_values('TotalProfit', ascending=False)
            .head(5)
            .reset_index()
        )
        # print("Top 5 Profitable Countries:")
        # print(tabulate(top_countries_df, headers='keys', tablefmt='grid'))  # Display the top countries data
    else:
        print("Column names do not match expected schema. Check column names in df_cleaned.")
        exit()
except Exception as e:
    print(f"Error during aggregation or column name check: {e}")
    exit()

# Create Gold Table 2 (Top 5 Profitable Countries)
try:
    session.execute("""
    CREATE TABLE IF NOT EXISTS medallionarchitecture.gold_sales_top_countries (
        Country text PRIMARY KEY,
        Total_Revenue float,
        Total_Profit float
    );
    """)
    print("Table 'gold_sales_top_countries' created successfully.")
except Exception as e:
    print(f"Error creating Gold Table 2: {e}")
    exit()

# Insert data into the Gold Table 2
try:
    insert_query = session.prepare("""
        INSERT INTO medallionarchitecture.gold_sales_top_countries (Country, Total_Revenue, Total_Profit) VALUES (?, ?, ?)
    """)

    for _, row in top_countries_df.iterrows():
        session.execute(insert_query, (row['Country'], row['TotalRevenue'], row['TotalProfit']))

    print("Gold table 2 populated with top 5 profitable countries.")
except Exception as e:
    print(f"Error inserting data into Gold Table 2: {e}")
    exit()

# Fetch and Display Data from Gold Table 2
try:
    rows = session.execute('SELECT * FROM medallionarchitecture.gold_sales_top_countries;')  # Fetch all rows
    # Convert rows to a Pandas DataFrame
    gold_countries_data = pd.DataFrame(list(rows), columns=['Country', 'Total_Revenue', 'Total_Profit'])

    # Display the data in a grid/tabular format
    print("Data from Gold Table 2 (Terminal View):")
    print(tabulate(gold_countries_data, headers='keys', tablefmt='grid'))  # For terminal

    # For Jupyter Notebook or Colab
    # print("Data from Gold Table 2 (Notebook View):")
    # display(gold_countries_data)  # For interactive table display in notebooks
except Exception as e:
    print(f"Error fetching Gold Table 2 data: {e}")


# Strip whitespace from column names
try:
    df_cleaned.columns = df_cleaned.columns.str.strip()
    print("Stripped whitespace from column names.")
    # print("Updated column names:")
    # print(tabulate(pd.DataFrame({'Columns': df_cleaned.columns}), headers='keys', tablefmt='grid'))
except Exception as e:
    print(f"Error while processing column names: {e}")
    exit()

# Check and process 'Order Date' column
try:
    if 'Order Date' in df_cleaned.columns:
        # Convert 'Order Date' to datetime
        df_cleaned['Order Date'] = pd.to_datetime(df_cleaned['Order Date'], errors='coerce')

        # Log and exclude rows with invalid dates
        if df_cleaned['Order Date'].isna().sum() > 0:
            print(f"Warning: Some rows have invalid dates: {df_cleaned[df_cleaned['Order Date'].isna()]}")
            df_cleaned = df_cleaned.dropna(subset=['Order Date'])

        # Add 'Month' column for monthly aggregation
        df_cleaned['Month'] = df_cleaned['Order Date'].dt.strftime('%Y-%m')

        print("Added 'Month' column for monthly aggregation.")
    else:
        print("Error: 'Order Date' column not found in the dataset.")
        exit()
except Exception as e:
    print(f"Error processing 'Order Date': {e}")
    exit()

# Check and process 'TotalRevenue' column
try:
    if 'TotalRevenue' in df_cleaned.columns:
        # Group by 'Month' and calculate Total Revenue
        monthly_df = df_cleaned.groupby('Month')[['TotalRevenue']].sum().reset_index()
        # print("Monthly Sales Trends:")
        # print(tabulate(monthly_df, headers='keys', tablefmt='grid'))  # Display monthly data
    else:
        print("Error: 'TotalRevenue' column not found in the dataset.")
        exit()
except Exception as e:
    print(f"Error processing 'TotalRevenue': {e}")
    exit()

# Create Gold Table for Monthly Sales Trends
try:
    session.execute("""
    CREATE TABLE IF NOT EXISTS medallionarchitecture.gold_sales_monthly (
        Month text PRIMARY KEY,
        TotalRevenue float
    );
    """)
    print("Table 'gold_sales_monthly' created successfully.")
except Exception as e:
    print(f"Error creating Gold Table 3: {e}")
    exit()

# Insert data into the Gold Table for Monthly Sales Trends
try:
    insert_query = session.prepare("""
        INSERT INTO medallionarchitecture.gold_sales_monthly (Month, TotalRevenue) VALUES (?, ?)
    """)

    for _, row in monthly_df.iterrows():
        session.execute(insert_query, (row['Month'], row['TotalRevenue']))

    print("Gold table 3 populated with monthly sales trends.")
except Exception as e:
    print(f"Error inserting data into Gold Table 3: {e}")
    exit()

# Fetch and Display Data from Gold Table for Monthly Sales Trends
try:
    rows = session.execute('SELECT * FROM medallionarchitecture.gold_sales_monthly;')  # Fetch all rows
    # Convert rows to a Pandas DataFrame
    gold_monthly_data = pd.DataFrame(list(rows), columns=['Month', 'TotalRevenue'])

    # Display the data in a grid/tabular format
    print("Data from Gold Table 3 (Terminal View):")
    print(tabulate(gold_monthly_data, headers='keys', tablefmt='grid'))  # For terminal

    # For Jupyter Notebook or Colab
    # print("Data from Gold Table 3 (Notebook View):")
    # display(gold_monthly_data)  # For interactive table display in notebooks
except Exception as e:
    print(f"Error fetching Gold Table 3 data: {e}")


# Query the Gold Table 3 and inspect rows
try:
    rows = session.execute("SELECT * FROM medallionarchitecture.gold_sales_monthly;")

    # Convert rows to a list of dictionaries for easier manipulation
    data = [{'Month': row.month, 'TotalRevenue': row.totalrevenue} for row in rows]

    # Sort the data by TotalRevenue in descending order
    sorted_data = sorted(data, key=lambda x: x['TotalRevenue'], reverse=True)

    # Convert sorted data to Pandas DataFrame for tabular visualization
    gold_monthly_df = pd.DataFrame(sorted_data)

    # Print in tabular format using tabulate
    print("\nTabular View of Gold Table 3 (Sorted by Total Revenue, Terminal):")
    print(tabulate(gold_monthly_df, headers='keys', tablefmt='grid'))  # Terminal-friendly grid format

    # For Jupyter Notebook or Colab
    # print("\nInteractive View of Gold Table 3 (Notebook):")
    # display(gold_monthly_df)  # Interactive display
except Exception as e:
    print(f"Error querying Gold Table 3: {e}")
