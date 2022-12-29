import csv
import numpy as np
import os
import duckdb

result_file = open('zipf_stats.csv', 'w')
csv_writer = csv.writer(result_file)
csv_writer.writerow(["zipf_param", "distinct", "distinct_perc", "num_ones", "num_ones_perc", "num_twos", "num_twos_perc", "num_threes", "num_threes_perc", "num_others", "num_others_perc"])

con = duckdb.connect(database=':memory:')

params = ["0_125", "0_25", "0_375", "0_5", "0_625", "0_75", "0_875", "1", "1_125", "1_25", "1_375", "1_5", "1_625", "1_75", "1_875", "2", "2_125", "2_25", "2_375", "2_5", "2_625", "2_75", "2_875", "3"]
total = 128000000

for param in params:

    print("param: " + param)

    con.execute("SELECT count(distinct zipf_key) from '/Users/tania/DuckDB/benchmarks/parquet/zipf_" + param + "_128M/zipf.parquet';")
    result = con.fetchall()
    distinct = result[0][0]
    distinct_perc = round(distinct/total, 4)

    con.execute("SELECT count(*) from '/Users/tania/DuckDB/benchmarks/parquet/zipf_" + param + "_128M/zipf.parquet' WHERE zipf_key = 1;")
    result = con.fetchall()
    num_ones = result[0][0]
    num_ones_perc = round(num_ones/total, 4)

    con.execute("SELECT count(*) from '/Users/tania/DuckDB/benchmarks/parquet/zipf_" + param + "_128M/zipf.parquet' WHERE zipf_key = 2;")
    result = con.fetchall()
    num_twos = result[0][0]
    num_twos_perc = round(num_twos/total, 4)

    con.execute("SELECT count(*) from '/Users/tania/DuckDB/benchmarks/parquet/zipf_" + param + "_128M/zipf.parquet' WHERE zipf_key = 3;")
    result = con.fetchall()
    num_threes = result[0][0]
    num_threes_perc = round(num_threes/total, 4)

    num_others = total - num_ones - num_twos - num_threes
    num_others_perc = round(num_others/total, 4)

    csv_writer.writerow([param, distinct, distinct_perc, num_ones, num_ones_perc, num_twos, num_twos_perc, num_threes, num_threes_perc, num_others, num_others_perc])

# close the file
result_file.close()
