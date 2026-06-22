from pyspark.sql import SparkSession, functions as F, types as T

RECEIPTS_PATH      = "data/receipt_restaurants"   # part-***** csv files, both 2021 and 2022
WEATHER_PATH       = "data/weather"
INITIAL_STATE_PATH = "data/initial_state"
OUTPUT_PATH        = "data/output"
CHECKPOINT_PATH    = "data/checkpoint"

RECEIPTS_SCHEMA = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("franchise_id", T.IntegerType()),
    T.StructField("franchise_name", T.StringType()),
    T.StructField("restaurant_franchise_id", T.IntegerType()),
    T.StructField("country", T.StringType()),
    T.StructField("city", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lng", T.DoubleType()),
    T.StructField("receipt_id", T.StringType()),
    T.StructField("total_cost", T.DoubleType()),
    T.StructField("discount", T.DoubleType()),
    T.StructField("date_time", T.StringType()),   # kept as string, parsed later with an explicit format
])

WEATHER_SCHEMA = T.StructType([
    T.StructField("lng", T.DoubleType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("avg_tmpr_c", T.DoubleType()),
    T.StructField("wthr_date", T.StringType()),    # yyyy-MM-dd
    T.StructField("city", T.StringType()),
    T.StructField("country", T.StringType()),
])

TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

def order_type_expr(size):
    return (
        F.when(size.isNull() | (size <= 0), F.lit("erroneous"))
             .when(size <= 1,  F.lit("tiny"))
             .when(size <= 3,  F.lit("small"))
             .when(size <= 10, F.lit("medium"))
             .otherwise(F.lit("large"))
    )

# Count rows that fell into a given bucket. Sum of 1/0 instead of count() so all five bucket counts can be produced in a single groupBy/agg pass
def _cnt(label):
    return F.sum(F.when(F.col("order_type") == label, F.lit(1)).otherwise(F.lit(0)))

# "Most popular" = the bucket with the highest count.
def most_popular_expr(tiny, small, medium, large):
    arr = F.array(
        F.struct(tiny.alias("c"), F.lit("tiny").alias("t")),
        F.struct(small.alias("c"), F.lit("small").alias("t")),
        F.struct(medium.alias("c"), F.lit("medium").alias("t")),
        F.struct(large.alias("c"), F.lit("large").alias("t")),
    )
    return F.sort_array(arr, asc=False)[0]["t"]


def enrich(df, weather_agg):
    # parse the timestamp once, then derive the bits we join/filter on
    df = (
        df.withColumn("ts", F.to_timestamp("date_time", TS_FMT))
            .withColumn("visit_date", F.to_date("ts"))
            # round coordinates to 2 dp so receipts and weather line up despite tiny
            # float differences (the task explicitly allows this)
            .withColumn("lat2", F.round("lat", 2))
            .withColumn("lng2", F.round("lng", 2))
    )

    enr = df.join(
        weather_agg,
        (df.lat2 == weather_agg.w_lat2) & (df.lng2 == weather_agg.w_lng2) & (df.visit_date == weather_agg.w_date),
        "left",
    )
    enr = enr.filter(F.col("avg_tmpr_c") > 0)  # keep only visits on days warmer than 0C
    enr = enr.withColumn("original_total_cost", F.col("total_cost") + F.col("discount"))
    enr = enr.withColumn("order_type", order_type_expr(F.col("total_cost") + F.col("discount")))
    # "restaurant" is just the franchise name, aliased so the grouping key reads cleanly
    return enr.withColumn("restaurant", F.col("franchise_name"))


def aggregate_state(enriched):
    return (enriched.groupBy("restaurant").agg(
        _cnt("erroneous").alias("erroneous_data_cnt"),
        _cnt("tiny").alias("tiny_cnt"),
        _cnt("small").alias("small_cnt"),
        _cnt("medium").alias("medium_cnt"),
        _cnt("large").alias("large_cnt"),
        F.avg("avg_tmpr_c").alias("avg_tmpr_c"),
    ))


spark = (SparkSession.builder
         .appName("RestaurantWeatherStreaming")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

weather_agg = (spark.read
               .schema(WEATHER_SCHEMA)
               .option("header", "true")
               .option("recursiveFileLookup", "true")   # handles the data/weather/weather nesting
               .csv(WEATHER_PATH)
               .withColumn("w_lat2", F.round("lat", 2))  # same 2 dp rounding as the receipts side
               .withColumn("w_lng2", F.round("lng", 2))
               .withColumn("w_date", F.to_date("wthr_date"))
               .groupBy("w_lat2", "w_lng2", "w_date")
               .agg(F.avg("avg_tmpr_c").alias("avg_tmpr_c")))
weather_agg = F.broadcast(weather_agg.cache())


# build the 2022 baseline ("initial state") and write it to disk
def build_initial_state():
    receipts = (
        spark.read
                .schema(RECEIPTS_SCHEMA)
                .option("header", "true")
                .csv(RECEIPTS_PATH)
    )

    # same enrich pipeline as the stream, just filtered to 2022
    enriched = enrich(receipts, weather_agg).filter(F.year("ts") == 2022)

    state = aggregate_state(enriched).drop("avg_tmpr_c")
    state = (
        state.withColumn("batch_timestamp", F.current_timestamp())
             .withColumn(
            "most_popular_order_type", most_popular_expr(F.col("tiny_cnt"), F.col("small_cnt"), F.col("medium_cnt"), F.col("large_cnt")))
             .select("restaurant", "batch_timestamp",
                     "erroneous_data_cnt", "tiny_cnt", "small_cnt",
                     "medium_cnt", "large_cnt", "most_popular_order_type"))

    (state.coalesce(1).write.mode("overwrite").option("header", "true").csv(INITIAL_STATE_PATH))
    print("Initial state written to", INITIAL_STATE_PATH)


# stream the 2021 receipts and fold them onto the baseline.
def run_stream():
    # read the baseline back from disk; we only need the counts to accumulate onto,
    initial_state = (spark.read.option("header", "true").option("inferSchema", "true")
                     .csv(INITIAL_STATE_PATH)
                     .select("restaurant",
                             "erroneous_data_cnt", "tiny_cnt", "small_cnt",
                             "medium_cnt", "large_cnt")).cache()
    initial_state.count()

    def process_batch(bdf, batch_id):
        enriched = enrich(bdf, weather_agg).filter(F.year("ts") == 2021)
        batch_state = aggregate_state(enriched)

        # add this batchs counts on top of the baseline's counts
        b = batch_state.alias("b")
        i = initial_state.alias("i")
        joined = b.join(i, "restaurant", "full_outer")

        # coalesce the nulls from the outer join to 0 before summing the two sides
        def total(col):
            return (F.coalesce(F.col("b." + col), F.lit(0)) + F.coalesce(F.col("i." + col), F.lit(0))).alias(col)

        combined = joined.select(
            F.col("restaurant"),
            total("erroneous_data_cnt"),
            total("tiny_cnt"),
            total("small_cnt"),
            total("medium_cnt"),
            total("large_cnt"),
            F.col("b.avg_tmpr_c").alias("avg_tmpr_c"),  # temp only exists on the 2021 side
        )

        final = (combined
                 .withColumn("promo_cold_drinks", F.coalesce(F.col("avg_tmpr_c"), F.lit(-1.0)) > 25.0)
                 .withColumn("batch_timestamp", F.current_timestamp())
                 .withColumn("most_popular_order_type",  most_popular_expr(F.col("tiny_cnt"), F.col("small_cnt"), F.col("medium_cnt"), F.col("large_cnt")))
                 .select("restaurant", "promo_cold_drinks", "batch_timestamp",
                         "erroneous_data_cnt", "tiny_cnt", "small_cnt",
                         "medium_cnt", "large_cnt", "most_popular_order_type"))

        # append: foreachBatch can fire more than once, so each batch adds to the folder
        (final.coalesce(1).write.mode("append").option("header", "true").csv(OUTPUT_PATH))

    stream = (spark.readStream.schema(RECEIPTS_SCHEMA).option("header", "true").csv(RECEIPTS_PATH))

    query = (stream.writeStream.foreachBatch(process_batch).option("checkpointLocation", CHECKPOINT_PATH)
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
    print("Final result written to", OUTPUT_PATH)


if __name__ == "__main__":
    build_initial_state()   # phase 1: write the 2022 baseline
    run_stream()            # phase 2: stream 2021 onto it and write the result
    spark.stop()
