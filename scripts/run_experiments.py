import csv
import numpy as np
import os
import duckdb
import time

os.system("rm ART_benchmarks.duckdb.wal")
os.system("rm ART_benchmarks.duckdb")
con = duckdb.connect(database='ART_benchmarks.duckdb')

def load_table(path, data_type):
    con.execute("CREATE TABLE temp_table (temp_key " + data_type + ");")
    con.execute("INSERT INTO temp_table SELECT * FROM 'parquet/" + path + "';")

    print("some cold runs on " + path)
    result = con.execute("SELECT COUNT(*) FROM temp_table;").fetchall()
    # change to unique index for sparse and dense
    con.execute("CREATE INDEX idx ON temp_table(temp_key);")
    con.execute("DROP INDEX idx;")
    print("count: " + str(result[0][0]))

def drop_table():
    con.execute("DROP TABLE temp_table;")

def run_bench():
    timings = []
    print("benchmarking...")
    for i in range(5):
        start = time.perf_counter()
        con.execute("CREATE INDEX idx ON temp_table(temp_key);")
        end = time.perf_counter()
        print("run " + str(i) + ": " + str(end - start))
        timings.append(end - start)
        con.execute("DROP INDEX idx;")

    # calculate timings
    return np.array(timings).astype(float)

def benchmark_threads(approach):

    print("benchmark number of threads...")
    file = open("bench_threads_approach_" + str(approach) + ".csv", 'w')
    csv_writer = csv.writer(file)
    csv_writer.writerow(["approach", "distribution", "num_threads", "mean", "median", "std"])

    paths = ["16M_dense/dense___m.parquet", "16M_sparse/sparse___m.parquet"]

    for num_threads in range(1, 9):
        print("num_threads: " + str(num_threads))

        for path_idx in range(2):
            print("path: " + paths[path_idx])
            con.execute("PRAGMA threads=" + str(num_threads) + ";")
            load_table(paths[path_idx], "BIGINT")

            # calculate timings
            np_timings = run_bench()
            mean = round(np.mean(np_timings), 4)
            median = round(np.median(np_timings), 4)
            std = round(np.std(np_timings), 4)

            # write timings
            print("mean: " + str(mean) + " median: " + str(median) + " std:" + str(std))
            distribution = "dense"
            if path_idx == 1:
                distribution = "sparse"
            csv_writer.writerow([approach, distribution, num_threads, mean, median, std])
            drop_table()

    file.close()

def benchmark_key_length(approach):

    print("benchmark key length...")
    file = open("bench_key_length_approach_" + str(approach) + ".csv", 'w')
    csv_writer = csv.writer(file)
    csv_writer.writerow(["approach", "sorted", "length", "mean", "median", "std"])

    paths = []
    # key_lengths = [8, 16, 32, 64, 128]
    # data_types = ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"]
    key_lengths = [128]
    data_types = ["HUGEINT"]
    for key_length in key_lengths:
        # paths.append("uniform_" + str(key_length) + "bit_128M/uniform.parquet")
        paths.append("uniform_" + str(key_length) + "bit_128M_sorted/sorted.parquet")

    data_type = 0
    for path in paths:
        print("path: " + path)
        load_table(path, data_types[int(data_type/2)])

        # calculate timings
        np_timings = run_bench()
        mean = round(np.mean(np_timings), 4)
        median = round(np.median(np_timings), 4)
        std = round(np.std(np_timings), 4)

        # write timings
        print("mean: " + str(mean) + " median: " + str(median) + " std:" + str(std))
        csv_writer.writerow([approach, data_type % 2, key_lengths[int(data_type/2)], mean, median, std])
        drop_table()
        data_type += 1

    file.close()

def benchmark_strings(approach):
    print("benchmark strings...")
    con.execute("PRAGMA threads=8;")
    file = open("bench_strings_approach_" + str(approach) + ".csv", 'w')
    csv_writer = csv.writer(file)
    csv_writer.writerow(["approach", "data_set", "mean", "median", "std"])

    con.execute("CREATE TABLE URLs (URL VARCHAR);")
    con.execute("INSERT INTO URLs SELECT * FROM 'parquet/URLs/urls.parquet';")

    print("some cold runs on URL data")
    result = con.execute("SELECT COUNT(*) FROM URLs;").fetchall()
    con.execute("CREATE INDEX idx ON URLs (URL);")
    con.execute("DROP INDEX idx;")
    print("count: " + str(result[0][0]))

    timings = []
    print("benchmarking URLs...")
    for i in range(5):
        start = time.perf_counter()
        con.execute("CREATE INDEX idx ON URLs (URL);")
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
    csv_writer.writerow([approach, "URLs", mean, median, std])
    con.execute("DROP TABLE URLs;")

    con.execute("CREATE TABLE wiki (namespace INTEGER, title VARCHAR);")
    con.execute("INSERT INTO wiki SELECT * FROM 'parquet/wiki/wiki.parquet';")

    print("some cold runs on wiki data")
    result = con.execute("SELECT COUNT(*) FROM wiki;").fetchall()
    con.execute("CREATE INDEX idx ON wiki (title);")
    con.execute("DROP INDEX idx;")
    print("count: " + str(result[0][0]))

    timings = []
    print("benchmarking wiki...")
    for i in range(5):
        start = time.perf_counter()
        con.execute("CREATE INDEX idx ON wiki (title);")
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
    csv_writer.writerow([approach, "wiki", mean, median, std])
    con.execute("DROP TABLE wiki;")

    file.close()

def benchmark_zipf(approach):

    print("benchmark zipf...")
    con.execute("PRAGMA threads=8;")
    file = open("bench_zipf_approach_" + str(approach) + "_0.csv", 'w')
    csv_writer = csv.writer(file)
    csv_writer.writerow(["approach", "zipf_parameter", "mean", "median", "std"])

    # zipf_parameters = ["0_125", "0_25", "0_375", "0_5", "0_625", "0_75", "0_875", "1", "1_125", "1_25", "1_375", "1_5", "1_625", "1_75", "1_875", "2", "2_125", "2_25", "2_375", "2_5", "2_625", "2_75", "2_875", "3"]
    # zipf_parameters = ["2_125", "2_25", "2_375", "2_5", "2_625", "2_75", "2_875", "3"]
    zipf_parameters = ["0_125", "2", "2_125", "2_25", "2_375", "2_5", "2_625", "2_75", "2_875", "3"]

    for zipf_param in zipf_parameters:
        path = "zipf_" + zipf_param + "_128M/zipf.parquet"
        print("path: " + path)
        load_table(path, "BIGINT")

        # calculate timings
        np_timings = run_bench()
        mean = round(np.mean(np_timings), 4)
        median = round(np.median(np_timings), 4)
        std = round(np.std(np_timings), 4)

        # write timings
        print("mean: " + str(mean) + " median: " + str(median) + " std:" + str(std))
        csv_writer.writerow([approach, zipf_param, mean, median, std])
        drop_table()

        if zipf_param == "1" or zipf_param == "2":
            file.close()
            file = open("bench_zipf_approach_" + str(approach) + "_" + zipf_param + ".csv", 'w')
            csv_writer = csv.writer(file)
            csv_writer.writerow(["approach", "zipf_parameter", "mean", "median", "std"])
    file.close()

# number is the currently compiled approach
benchmark_threads(3)
# benchmark_key_length(3)
# benchmark_strings(3)
# benchmark_zipf(3)
