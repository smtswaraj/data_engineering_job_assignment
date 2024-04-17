# Solution using purely SQL
import sqlite3
import csv

# Connect to SQLite database
conn = sqlite3.connect('Data Engineer_ETL Assignment.db')
cursor = conn.cursor()

# SQL query to get total quantities of each item bought per customer aged 18-35
sql_query = """
                SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) as total_quantity
                FROM Customers c
                JOIN Sales s ON c.customer_id = s.customer_id
                JOIN Orders o ON s.sales_id = o.sales_id
                JOIN Items i ON o.item_id = i.item_id
                WHERE c.age BETWEEN 18 AND 35
                GROUP BY c.customer_id, i.item_name
                HAVING total_quantity > 0
            """

# Execute SQL query
cursor.execute(sql_query)

# Fetch all results
results = cursor.fetchall()

# Write results to CSV file
with open('output_sql.csv', 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
    csv_writer.writerows(results)

# Close database connection
conn.close()


# Solution using Pandas
import pandas as pd

# Read data from SQLite database into DataFrame
conn = sqlite3.connect('Data Engineer_ETL Assignment.db')
df = pd.read_sql_query(sql_query, conn)

# Write DataFrame to CSV file
df.to_csv('output_pandas.csv',index=False)

# Close database connection
conn.close()
