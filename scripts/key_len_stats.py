import csv
import numpy as np
import os
import duckdb

result_file = open('key_len_stats.csv', 'w')
csv_writer = csv.writer(result_file)
csv_writer.writerow(["key_length", "distinct", "distinct_perc", "min", "max", "avg", "median", "stddev_samp"])

con = duckdb.connect(database=':memory:')

params = [8, 16, 32, 64, 128]
total = 128000000

for param in params:

    print("param: " + str(param))

    con.execute("SELECT count(distinct sorted_key) from '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(param) + "bit_128M_sorted/sorted.parquet';")
    result = con.fetchall()
    distinct = result[0][0]
    distinct_perc = round(distinct/total, 4)

    con.execute("SELECT min(sorted_key) from '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(param) + "bit_128M_sorted/sorted.parquet';")
    result = con.fetchall()
    min = result[0][0]

    con.execute("SELECT max(sorted_key) from '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(param) + "bit_128M_sorted/sorted.parquet';")
    result = con.fetchall()
    max = result[0][0]

    con.execute("SELECT avg(sorted_key) from '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(param) + "bit_128M_sorted/sorted.parquet';")
    result = con.fetchall()
    avg = result[0][0]

    con.execute("SELECT median(sorted_key) from '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(param) + "bit_128M_sorted/sorted.parquet';")
    result = con.fetchall()
    median = result[0][0]

    con.execute("SELECT stddev_samp(sorted_key) from '/Users/tania/DuckDB/benchmarks/parquet/uniform_" + str(param) + "bit_128M_sorted/sorted.parquet';")
    result = con.fetchall()
    stddev_samp = result[0][0]

    csv_writer.writerow([str(param) + "bit", distinct, distinct_perc, min, max, avg, median, stddev_samp])

# close the file
result_file.close()
