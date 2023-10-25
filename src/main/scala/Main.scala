import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}

object Main {
  def main(args: Array[String]): Unit = {

    //remove unnecessary WARN and INFO messages from RUN output
    Logger.getLogger("org").setLevel(Level.ERROR)

    //create SaprkSession
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("ECB_task")
      .getOrCreate()
//
//    //println("Spark version: "+spark.version)
//
    //import CSV file to a DataFrame
    val df = spark.read.option("delimiter", ",")
      .option("header", "true").csv("src/resources/transaction_test_data.csv")

    df.show(20)

    //Filtering
    val filter_trans_below = df.filter(df("TransactionAmount") >= 500)
    val filter_trans_above = df.filter(df("TransactionAmount") <= 500)
    val filter_user = df.filter(df("UserID") === "U75")

    //transform timestamp to date format to apply filtering
    val filter_date = df.withColumn("Timestamp", to_date(df("Timestamp"), "MM/dd/yyyy"))
    val filter_date_mix = filter_date.filter(filter_date("Timestamp") > lit("2022-10-22"))

    filter_trans_below.show(20)
    filter_trans_below.show(20)
    filter_user.show(20)
    filter_date_mix.show(20)

    //Sorting

    //sort ascending (from small to big)
    val sorted_df = df.sort(df("TransactionAmount"))

    //sort descending (from big to small)
    val sorted_desc_df = df.sort(df("TransactionAmount").desc)

    sorted_df.show(20)
    sorted_desc_df.show(20)

    //Aggregation


    //Count total transaction amount for each user
    val total_user = df.groupBy(df("UserID")).agg(round(sum("TransactionAmount"),2).alias("UserTotal"))
    total_user.show(20)


    //Joining


    // create mini DataFrame to apply join

    val data = Seq(("U52","SEB"),("U72","SwedBank"),("U75","Luminor"))
    val rdd = spark.sparkContext.parallelize(data)
    val second_df = spark.createDataFrame(rdd).toDF("UserID","Bank")

    second_df.show()

    //join original DataFrame with mini DataFrame
    val joined_df = df.join(second_df, Seq("UserID")).sort(df("TransactionID"))
    joined_df.show(20)

    //Column transformations

    //Calculate average to use in when case, the result is 496.61
    val average = df.groupBy().agg(round(avg("TransactionAmount"),2))
    average.show()

    //Apply CASE in DataFrame to assign values
    val case_df = df.withColumn("Value", when(df("TransactionAmount")>=496.61,"High Value").otherwise("Low Value"))
    case_df.show(20)

    //Additional CASE usage with multiple WHEN statements

    //Count all user transactions
    val count = df.groupBy("UserID").count()
    count.show()

    //Apply CASE to assign tier for each user
    val case_count = count.withColumn("Tier", when(count("count") >=95 && count("count") < 110, "Bronze Client")
      .when(count("count") >=110 && count("count") < 125, "Silver Client")
      .when(count("count") >=125, "Golden Client")
      .otherwise("New Client"))

    case_count.show(20)

    //Additionally count how many users are in each tier
    val tier_count = case_count.groupBy("Tier").count()
    tier_count.show()

    //Normalization

    /* In order to calculate normalization
       we need mean and standard deviation
       Used formula => Normalized Value = (x - Mean) / Standard Deviation
    */
    val df_mean = df.groupBy().agg(mean(df("TransactionAmount"))).first().getDouble(0)
    val df_standard = df.groupBy().agg(stddev(df("TransactionAmount"))).first().getDouble(0)

    val norm_df = df.withColumn("NormalizedTransAmount", round((df("TransactionAmount")-df_mean)/df_standard,4))
    norm_df.show()

    //Dealing with Duplicates

    //For a simple DataFrame with no unique ID for each record (not our case)

    // First we can see if any duplicates exist by using distinct and count
    // original DataFrame has:
    println("df record count is " + df.count())
    //after applying distinct
    val cleaned_df = df.distinct()
    println("clean df record count is " + cleaned_df.count())

    //If DataFrame has duplicates the cleaned DataFrame record quantity will be smaller
    //We can remove the duplicates by using the dropDuplicates() method
    println("df record count before dropping duplicates is " + df.count())
    val noDup_df = df.dropDuplicates()
    println("df record after dropping duplicates count is " + noDup_df.count())

    //For our case

    // Since the TransactionID is always unique, the only values that can duplicate
    // are UserID, TransactionAmount and Timestamp
    // Here we have to look at the combination of all 3 col together
    // Since User + Amount = can have different timestamp, not duplicate
    // User + Timestamp = theoretically can transfer different amount at the same time, not duplicate
    // and Amount + Timestamp = can have different user, not duplicate

    //First we need to separate the Dataframe from TransactionID using window
    val windowSpec = Window.partitionBy("UserID", "TransactionAmount", "Timestamp")

    // We count the rows of the window where 3 selected columns are same
    // After it we filter the rows, so it shows us only duplicates, because it counted more than one similar rows
    // Lastly we remove the "count" column
    // This allows us to see what are the duplicates
    val duplicates_df = df
      .withColumn("count", count("*").over(windowSpec))
      .filter(col("count") > 1)
      .drop("count")

    duplicates_df.show()

    // Now we see the duplicate ID's and notice the pattern
    // We can now remove them with filtering and reassigning original Dataframe if necessary
    val filtered_df = df.filter(!df("TransactionID").startsWith("DUP"))
    filtered_df.show()


    //Optimization

    //API selection
    //It is best to use Dataframe < DataSet < RDD

    //Broadcasting
    /* When planning on combining two Dataframes, where one is small and the others is big
       It is best to use broadcast join
       So in my previous join task, it would be better to use broadcast for my custom mini dataframe
     */

    // create mini DataFrame to apply join

//    val data = Seq(("U52","SEB"),("U72","SwedBank"),("U75","Luminor"))
//    val rdd = spark.sparkContext.parallelize(data)
//    val second_df = spark.createDataFrame(rdd).toDF("UserID","Bank")
//
//    second_df.show()

    //join original DataFrame with mini DataFrame, but now using broadcast join!
    val broadcasted_df = df.join(broadcast(second_df), Seq("UserID"))
    broadcasted_df.show(20)

    //Caching
    /* We use cache() on a Dataframe, when we plan on working with it multiple times.
       Like apply groupBy, agg, filter etc.
       Since each of these might trigger re-computation of data
     */

    //Experiment zone

    val start_noCache = System.nanoTime()

    val transformed_df = df.groupBy("UserID")

    val end_noCache = System.nanoTime()

    val res_noCache = (end_noCache-start_noCache)/1e9
    println("Elapsed time for not cached Dataframe: " + res_noCache)

    val start_Cache = System.nanoTime()

    val cached_df = df.cache()

    val transform_cache = cached_df.groupBy("UserID").count()

    val end_Cache = System.nanoTime()

    val res_Cache = (end_Cache - start_Cache) / 1e9
    println("Elapsed time for cached Dataframe: " + res_Cache)

    //The results show, that the cached Dataframe needs more time, than the not cached Dataframe :/

    //Use reduceByKey not groupByKey
    /* The main problem with groupByKey is, that it's shuffling the data
       reduceByKey is first applying all the necessary transformations< this way
       reducing the amount of final shuffling
     */

    //File format
    //Although spark supports many formats, the most optimized format is parquet

    //Partitioning
    /* Partitioning is base in Data Engineering. It allows us to split big data into smaller
       chunks and then use separately. This way we can perform more transformations in parallel
       It's more usable when you have multiple disks or pc
     */

    //Coalescing and Repartitioning
    /* Coalescing in Spark reduces the amount of partitions
       And Repartitioning just changes the amount of partitions that was made
       Like if i need 5 partitions not 2 or 8, i can repartition them
       But repartitioning uses shuffling, so if we want to reduce partition amoun from 5 to 2
       It's best to use coalesce()
     */



  }
}