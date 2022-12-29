import csv
import numpy as np
import os
import duckdb
import time

# same, just change names, for dense

os.system("rm bench-initial.duckdb.wal")
os.system("rm bench-initial.duckdb")

print("setting up the db...")
con = duckdb.connect(database='bench-initial.duckdb')
con.execute("PRAGMA threads=8;")
con.execute("CREATE TABLE temp_table (temp_key " + "BIGINT" + ");")
con.execute("INSERT INTO temp_table SELECT * FROM 'parquet/" + "128M_sparse/sparse____m.parquet" + "';")

print("some cold runs on sparse data")
result = con.execute("SELECT COUNT(*) FROM temp_table;").fetchall()
con.execute("CREATE UNIQUE INDEX idx ON temp_table(temp_key);")
con.execute("DROP INDEX idx;")
print("count: " + str(result[0][0]))

timings = []
print("benchmarking sparse data...")
for i in range(5):
    start = time.perf_counter()
    con.execute("CREATE UNIQUE INDEX idx ON temp_table(temp_key);")
    end = time.perf_counter()
    print("run " + str(i) + ": " + str(end - start))
    timings.append(end - start)
    con.execute("DROP INDEX idx;")

# calculate timings
np_timings = np.array(timings).astype(float)
mean = round(np.mean(np_timings), 4)
median = round(np.median(np_timings), 4)
std = round(np.std(np_timings), 4)

# write timings
print("mean: " + str(mean) + " median: " + str(median) + " std:" + str(std))
