cd /Users/rizalmaulana/Documents/airflow_workspace/airflow/dags/task3

great_expectations --v3-api checkpoint run new_sales
great_expectations --v3-api checkpoint run new_stores
great_expectations --v3-api checkpoint run new_inventory
great_expectations --v3-api checkpoint run new_products

echo "Done"