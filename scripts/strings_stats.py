import csv
import numpy as np
import os
import duckdb

# copy paste hell

result_file = open('strings_stats.csv', 'w')
csv_writer = csv.writer(result_file)
csv_writer.writerow(["name", "count", "distinct", "distinct_perc", "min", "max", "avg", "median", "stddev_samp"])

con = duckdb.connect(database=':memory:')

print("URLs")

con.execute("SELECT count(URL) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
count = result[0][0]

con.execute("SELECT count(distinct URL) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
distinct = result[0][0]
distinct_perc = round(distinct/count, 4)

con.execute("SELECT min(length(URL)) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
min = result[0][0]

con.execute("SELECT max(length(URL)) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
max = result[0][0]

con.execute("SELECT avg(length(URL)) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
avg = result[0][0]

con.execute("SELECT median(length(URL)) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
median = result[0][0]

con.execute("SELECT stddev_samp(length(URL)) from '/Users/tania/DuckDB/benchmarks/parquet/URLs/urls.parquet';")
result = con.fetchall()
stddev_samp = result[0][0]

csv_writer.writerow(["URLs", count, distinct, distinct_perc, min, max, avg, median, stddev_samp])

print("wiki")

con.execute("SELECT count(title) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
count = result[0][0]

con.execute("SELECT count(distinct title) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
distinct = result[0][0]
distinct_perc = round(distinct/count, 4)

con.execute("SELECT min(length(title)) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
min = result[0][0]

con.execute("SELECT max(length(title)) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
max = result[0][0]

con.execute("SELECT avg(length(title)) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
avg = result[0][0]

con.execute("SELECT median(length(title)) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
median = result[0][0]

con.execute("SELECT stddev_samp(length(title)) from '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';")
result = con.fetchall()
stddev_samp = result[0][0]

csv_writer.writerow(["wiki", count, distinct, distinct_perc, min, max, avg, median, stddev_samp])

# close the file
result_file.close()
