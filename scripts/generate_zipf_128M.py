import csv
import numpy as np
import os
import duckdb
import time

os.system("rm ART_benchmarks.duckdb.wal")
os.system("rm ART_benchmarks.duckdb")
con = duckdb.connect(database='ART_benchmarks.duckdb')

zipf_parameters = ["0_375", "0_5", "0_625", "0_75", "0_875", "1", "1_125", "1_25", "1_375", "1_5", "1_625", "1_75", "1_875", "2", "2_125", "2_25", "2_375", "2_5", "2_625", "2_75", "2_875", "3"]

for zipf_param in zipf_parameters:
    print(zipf_param)
    con.execute("CREATE TABLE zipf(zipf_key BIGINT);")
    con.execute("INSERT INTO zipf SELECT * FROM 'parquet/zipf_" + zipf_param + "/zipf.parquet' LIMIT 128000000;")
    con.execute("EXPORT DATABASE '/Users/tania/DuckDB/benchmarks/parquet/zipf_" + zipf_param + "_128M' (FORMAT PARQUET);")
    con.execute("DROP TABLE zipf;")
