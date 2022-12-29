#!python3
import os
import shutil
import subprocess
import xml.etree.ElementTree as ET

sql_zipf = "CREATE TABLE temp_table (temp_key BIGINT);INSERT INTO temp_table SELECT * FROM '/Users/tania/DuckDB/benchmarks/parquet/zipf_1_5_128M/zipf.parquet';CREATE INDEX idx ON temp_table(temp_key);DROP INDEX idx;CREATE INDEX idx ON temp_table(temp_key);"
sql_key_length_sorted = "CREATE TABLE temp_table (temp_key INTEGER);INSERT INTO temp_table SELECT * FROM '/Users/tania/DuckDB/benchmarks/parquet/uniform_32bit_128M_sorted/sorted.parquet';CREATE INDEX idx ON temp_table(temp_key);DROP INDEX idx;CREATE INDEX idx ON temp_table(temp_key);"
sql_key_length = "CREATE TABLE temp_table (temp_key INTEGER);INSERT INTO temp_table SELECT * FROM '/Users/tania/DuckDB/benchmarks/parquet/uniform_32bit_128M/uniform.parquet';CREATE INDEX idx ON temp_table(temp_key);DROP INDEX idx;CREATE INDEX idx ON temp_table(temp_key);"
sql_threads_dense = "CREATE TABLE temp_table (temp_key BIGINT);INSERT INTO temp_table SELECT * FROM '/Users/tania/DuckDB/benchmarks/parquet/128M_dense/dense____m.parquet';PRAGMA threads=4;CREATE UNIQUE INDEX idx ON temp_table(temp_key);DROP INDEX idx;CREATE UNIQUE INDEX idx ON temp_table(temp_key);"
sql_threads_sparse ="CREATE TABLE temp_table (temp_key BIGINT);INSERT INTO temp_table SELECT * FROM '/Users/tania/DuckDB/benchmarks/parquet/128M_sparse/sparse____m.parquet';PRAGMA threads=4;CREATE UNIQUE INDEX idx ON temp_table(temp_key);DROP INDEX idx;CREATE UNIQUE INDEX idx ON temp_table(temp_key);"
sql_wiki = "CREATE TABLE wiki (namespace INTEGER, title VARCHAR);INSERT INTO wiki SELECT * FROM '/Users/tania/DuckDB/benchmarks/parquet/wiki/wiki.parquet';CREATE INDEX idx ON wiki (title);DROP INDEX idx;CREATE INDEX idx ON wiki (title);"

csv_names = ["zipf", "key_length_sorted", "key_length", "threads_dense", "threads_sparse", "wiki"]
sql_queries = [sql_zipf, sql_key_length_sorted, sql_key_length, sql_threads_dense, sql_threads_sparse, sql_wiki]

XPATH = '/trace-toc/run[@number="1"]/data/table[@schema="counters-profile"]'
EXPORT_CMD = f'xctrace export --input sim.trace --xpath \'{XPATH}\' > trace.xml'
RECORD_CMD = 'xcrun xctrace record --template counters.tracetemplate --output "sim.trace" --launch -- /Users/tania/DuckDB/duckdb/build/release/duckdb -c "'


def create_csv_header(csv):
    print("TIME,FIXED_CYCLES,FIXED_INSTRUCTIONS,L1D_CACHE_MISS_ST,L1D_CACHE_MISS_LD,BRANCH_MISPRED_NONSPEC", file=csv)
    csv.flush()


def trace(query):
    if os.path.exists('sim.trace'):
        shutil.rmtree('sim.trace')
    cmd = RECORD_CMD + query + '"'
    subprocess.run(cmd, shell=True, capture_output=True)


def append_run(csv):
    subprocess.run(EXPORT_CMD, shell=True, capture_output=True)
    tree = ET.parse('trace.xml')
    root = tree.getroot()
    for row in root[0].findall('.//row'):
        pmc = row.find('pmc-events')
        if not pmc.text:
            continue
        if not row.find('sample-time').text:
            continue
        print(f"{row.find('sample-time').text},{pmc.text.replace(' ', ',')}", file=csv)
    csv.flush()
    shutil.rmtree('sim.trace')
    os.remove('trace.xml')


def main():
    for i in range(6):
        print(csv_names[i])
        fname = f'counters_ap2_' + csv_names[i] + '.csv'
        with open(fname, 'w+') as f:
            create_csv_header(f)
            print("get trace")
            trace(sql_queries[i])
            print("write CSV")
            append_run(f)

if __name__ == '__main__':
    main()
