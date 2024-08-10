from pyspark.sql import SparkSession
import sys

LOAD_DT = sys.argv[1]

spark = SparkSession.builder.appName("movie_sum").getOrCreate()

df1 = spark.read.parquet(f"/home/hun/data/movie/hive/load_dt={LOAD_DT}")
df1.createOrReplaceTempView("oneday_comers")

df_n = spark.sql(f"""
SELECT
    sum(saleAmt) as sum_saleAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    multiMovieYn,
    '{LOAD_DT}' AS load_dt
FROM oneday_comers
GROUP BY multiMovieYn
""")
df_n.write.mode('append').partitionBy("load_dt").parquet("/home/hun/data/movie/sum-multi")

df_r = spark.sql(f"""
SELECT
    sum(saleAmt) as sum_saleAmt,
    sum(audiCnt) as sum_audiCnt,
    sum(showCnt) as sum_showCnt,
    repNationCd,
    '{LOAD_DT}' AS load_dt
FROM oneday_comers
GROUP BY repNationCd
""")

df_r.write.mode('append').partitionBy("load_dt").parquet("/home/hun/data/movie/sum-nation")

spark.stop()
