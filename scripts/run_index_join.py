import csv
import numpy as np
import os
import duckdb
import time

os.system("rm tpch-bench.duckdb.wal")
os.system("rm tpch-bench.duckdb")

print("setting up the db...")
con = duckdb.connect(database='tpch-bench.duckdb')
con.execute("CREATE TABLE lineitem AS SELECT * FROM 'tpch-bench-parquet/sf1/lineitem.parquet';")
con.execute("CREATE TABLE orders AS SELECT * FROM 'tpch-bench-parquet/sf1/orders.parquet';")
con.execute("CREATE UNIQUE INDEX pk ON orders(o_orderkey);")
con.execute("PRAGMA force_index_join;")

query = "SELECT avg(l_quantity), sum(L_EXTENDEDPRICE), min(L_DISCOUNT + L_TAX), sum(O_TOTALPRICE) FROM lineitem JOIN orders ON (o_orderkey = l_orderkey);"

print("cold run...")
file = open("bench_index_join.csv", 'w')
csv_writer = csv.writer(file)
csv_writer.writerow(["mean", "median", "std"])

# warm runs
timings = []
for j in range(5):
    start = time.perf_counter()
    con.execute(query )
    end = time.perf_counter()
    print("run " + str(j) + ": " + str(end - start))
    timings.append(end - start)

np_timings = np.array(timings).astype(float)
mean = round(np.mean(np_timings), 4)
median = round(np.median(np_timings), 4)
std = round(np.std(np_timings), 4)
print("mean: " + str(mean) + " median: " + str(median) + " std:" + str(std))
csv_writer.writerow([mean, median, std])
file.close()
