from sqlalchemy import create_engine
import dask.dataframe as dd

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

engine.connect() 

ddf = dd.read_parquet("yellow_tripdata_2021-01.parquet")

chunksize = 300000

def insert_chunk(df_chunk, table_name):
    df_chunk.to_sql(table_name, con=engine, if_exists='append', index=False)

for start in range(0, len(ddf), chunksize):
    print(start)
    chunk = ddf.loc[start:start+chunksize]
    insert_chunk(chunk.compute(), 'yellow_taxi_data')