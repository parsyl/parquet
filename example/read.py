import pyarrow.parquet as pq

def main():
    table = pq.read_table('records.parquet')
    print table.to_pandas()

if __name__ == "__main__":
    main()
