You can check the new table on both database on create_db_sales.sql and create_db_employees.sql. For the DAG I'm using python operator to get the data and ingest the data. Because the date is different so i have two different DAG for both task. To processing this into daily task, I used date on both database to determine which data should be ingest at that time.