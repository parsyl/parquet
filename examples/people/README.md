# Parquetgen example


To generate the code needed to run this example:

    cd examples/people
    parquetgen -type Person -package main
    go run .

You should now have a parquet file that encodes all the people in
people.json.
