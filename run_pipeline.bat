@echo off
echo Running FinFlow pipeline...

call "C:\Users\ruhan\AppData\Local\Programs\Python\Python311\python.exe" "C:\Users\ruhan\OneDrive\Desktop\finflow\ingestion\ingest_stocks.py"

call "C:\Users\ruhan\AppData\Local\Programs\Python\Python311\python.exe" "C:\Users\ruhan\OneDrive\Desktop\finflow\ingestion\ingest_macro.py"

cd "C:\Users\ruhan\OneDrive\Desktop\finflow\finflow_dbt"

call "C:\Users\ruhan\AppData\Local\Programs\Python\Python311\Scripts\dbt.exe" run

echo Pipeline complete.
