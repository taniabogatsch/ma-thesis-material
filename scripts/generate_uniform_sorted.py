import csv
import numpy as np
import os
import duckdb
import time

os.system("rm ART_benchmarks.duckdb.wal")
os.system("rm ART_benchmarks.duckdb")
con = duckdb.connect(database='ART_benchmarks.duckdb')

params = [8, 16, 32, 64, 128]
dtypes = ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"]

for i in range(5):
    print(params[i])
    con.execute("CREATE TABLE sorted (sorted_key " + dtypes[i] + ");")
    con.execute("INSERT INTO sorted SELECT * FROM 'parquet/uniform_" + str(params[i]) + "bit_128M/uniform.parquet' ORDER BY uniform_key;")
    con.execute("EXPORT DATABASE '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(params[i]) + "bit_128M_sorted' (FORMAT PARQUET);")
    con.execute("DROP TABLE sorted;")
