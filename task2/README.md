Because there are three database that need to be created, I use three different task in airflow which is for daily, monthly, and annual. For the most part the code between three task doesn't have that many different, the different that monthly and annualy will checked if current date (logical date) on the first of the month or year respectively.