import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd

def main():
    df = pd.read_json("./records.json")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'records.parquet')

if __name__ == "__main__":
    main()
