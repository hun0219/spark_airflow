import pandas as pd
import os

def repartition(load_dt, from_path='/data/movie_data'):
    home_dir = os.path.expanduser("~")
    read_path = f'{home_dir}/{from_path}/load_dt={load_dt}'
    df = pd.read_parquet(read_path)
    df['load_dt'] = load_dt
    df.to_parquet(write_path, partition_cols=['movieCd','multiMovieYn','repNationCd'])
    retrun df.size, read_path, f'{write_path}/load_dt={load_dt}'
