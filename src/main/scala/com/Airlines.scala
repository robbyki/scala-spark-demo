package com

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import functions._
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.Window

object Airlines {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("airlines")
      .config("spark.debug.maxToStringFields", "5000")
      .getOrCreate()

    // these keys are exported as gpg env keys with my sessions
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.access.key", sys.env.get("AWS_ACCESS_KEY_ID").get)
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.secret.key", sys.env.get("AWS_SECRET_ACCESS_KEY").get)

//------------------------------------------------------------------//
//                       BEGIN NOTEBOOK HERE                        //
//------------------------------------------------------------------//

    /**
     * What you'll do:
     *
     * We provide the dataset. You will load it into dataframes, and perform some data cleansing and
     * transformation tasks. You will answer a series of questions to show insights from the data.
     * There are also some written-answer questions. We care about the process, not the result.
     * I.e., we're looking for proper use of data engineering techniques and understanding of the
     * code you've written.
     *
     * Using Airlines Data Set The following questions use the airlines dataset located at
     * dbfs:/interview-datasets/sa/airlines. All airlines questions expect answers that use the
     * Dataframes API (Scala or Python). SQL only answers are accepted but may receive reduced
     * points. We will not accept answers that use the RDD API.
     *
     * This Data Engineering section is scored out of 55 points.
     *
     * **Part A:** Airlines Question 1 [5 Points]
     *   - Write code that uses the DataFrame API to read in the entire airlines data set with
     *     clearly named columns.
     *
     * **Part B:** Airlines Question 2 [5 Points]
     *   - How many unique airlines are present in this dataset?
     *
     * **Part C:** Airlines Question 3 [10 Points]
     *   - Which airline is delayed on departure most often? Show a bar graph of the top five most
     *     delayed airlines.
     *
     * **Part D:** Airlines Question 4 [15 Points]
     *   - Part a: What was the average arrival delay per airline?
     *   - Part b: Also, for each airline, on average did flights arrive early or late?
     *   - Part c: Add a column to this new dataframe (containing the grouped averages) that
     *     contains the string "Late" if the average arrival for that airline arrive >15 minutes
     *     late, "Early" if the average arrival is <0 minutes late, and "On-time" if the average
     *     arrival is between 0 and 15 minutes late.
     *
     * Important -> To add the additional column, use a Spark UDF. Additionally, make sure to filter
     * out or fill in null values in your dataframe (if there are any) prior to applying the UDF.
     *
     * **Part E:** Airlines Question 5 [15 Points]
     *   - What file format is airlines data stored in, and was this the most optimal format for the
     *     questions asked above?
     *   - What format would you store this data in if you frequently queried only the UniqueCarr
     *     and CancellationCode columns?
     *   - What if you frequently read entire rows of the dataset?
     *
     * Note: Cite any sources used. You do not need a code answer for this question.
     *
     * **Part F:** Airlines Question 6 [5 Points]
     *   - If you needed to keep multiple versions of this dataset, why might you use the Delta
     *     format to do this efficiently?
     */

    /**
     * Some Helper Functions I will use
     */

    import spark.implicits._

    /**
     * takes a StructType and applies a schema with `read.schema()`
     *
     * @param schema
     *   in StructType format
     * @return
     *   a DataFrame with the schema
     */
    def dataFromSchema(schema: Option[StructType]): DataFrameReader =
      schema.fold(spark.read)(s => spark.read.schema(s))

    /**
     * A helper function for reading data in csv format with relevant parameters available to caller
     *
     * @param path:
     *   seq of path files to read
     * @param delim:
     *   delimiter to use for parsing CSV
     * @param hasHeader:
     *   whether or not file already has a header
     * @param inferSchema:
     *   should spark infer the schema from the sounrce file
     * @param schema:
     *   the schema to apply or to ignore and read as is
     * @param encoding:
     *   encoding to use
     * @return:
     *   Dataframe format from csv data
     */
    def readCsv(
      path: Seq[String],
      delim: String = ",",
      hasHeader: Boolean = true,
      inferSchema: Boolean = true,
      schema: Option[StructType] = None,
      encoding: String = "UTF-8"
    ): Option[DataFrame] = {
      def buildOptionsMap(
        delim: String,
        hasHeader: Boolean,
        inferSchema: Boolean,
        encoding: String
      ): Map[String, String] = Map(
        "sep"                         -> delim,
        "inferSchema"                 -> inferSchema.toString(),
        "encoding"                    -> encoding.toString(),
        "escape"                      -> "\"",
        "comment"                     -> "",
        "header"                      -> hasHeader.toString,
        "ignoreLeadingWhiteSpace"     -> "false",
        "ignoreTrailingWhiteSpace"    -> "false",
        "nullValue"                   -> "",
        "nanValue"                    -> "NaN",
        "positiveInf"                 -> "Inf",
        "negativeInf"                 -> "-Inf",
        "dateFormat"                  -> "yyyy-MM-dd",
        "timestampFormat"             -> "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        "maxColumns"                  -> "2048",
        "maxCharsPerColumn"           -> "-1",
        "maxMalformedLogPerPartition" -> "10",
        "mode"                        -> "PERMISSIVE"
      )
      try schema
        .map(_ => dataFromSchema(schema))
        .orElse(Some(spark.read))
        .map(
          _.format("csv")
            .options(buildOptionsMap(delim, hasHeader, inferSchema, encoding))
            .load(path: _*)
        )
      catch {
        case e: Exception =>
          e.printStackTrace()
          None
      }
    }

    def getDistinctVals(df: DataFrame): DataFrame =
      df.select(df.columns.map(c => collect_set(col(c)).alias(c)): _*)

    /**
     * Cleans dataframe header by applying a regex and converting to upper case along with replacing
     * spacing with underscore.
     *   - Converts all field names to lowercase to standardize
     *   - Removes any extra space with `trim`
     *   - sorts the columns to make it a lot easier to work with later
     * @param df
     *   dataframe
     */
    def cleanHeader()(df: DataFrame): DataFrame = {
      def fixChars(str: String): String = str
        .toLowerCase() // I know...redundant and duplicated
        .replace(" ", "")
        .replaceAll("[\\.$|,|;|'|\\s|#]", "")
      def lowerCaseCols()(df: DataFrame): DataFrame = {
        def fixChars(str: String): String = str.toLowerCase()
        df.columns.foldLeft(df)((transformDF, colName) =>
          transformDF.withColumnRenamed(colName, fixChars(colName))
        )
      }
      def trimCols()(df: DataFrame): DataFrame =
        df.select(df.columns.map(c => trim(col(c)).alias(c)): _*)
      def sortCols()(df: DataFrame): DataFrame =
        df.select(df.columns.sorted.head, df.columns.sorted.tail: _*)
      df.columns
        .foldLeft(df) { (transformDF, colName) =>
          transformDF
            .withColumnRenamed(colName, fixChars(colName))
        }
        .transform(sortCols())
        .transform(trimCols())
        .transform(lowerCaseCols())
    }

    /**
     * The goal with this is to simply remove entire columns that have only null values. This is
     * also merely to simplify analyisis and have cleaner data. Another option of course is to just
     * use projection on reading and use a subset of columns which would be highly efficient and
     * optimized to do against properly partitioned data
     *
     * @param df
     * @return
     */
    def dropAllNACols(df: DataFrame) = {
      val row = df
        .select(df.columns.map(c => when(col(c).isNull, 0).otherwise(1).as(c)): _*)
        .groupBy()
        .max(df.columns.map(c => c): _*)
        .first
      // and filter the columns out
      val colKeep = row
        .getValuesMap[Int](row.schema.fieldNames)
        .map(c => if (c._2 == 1) Some(c._1) else None)
        .flatten
        .toArray
      val delaysNoNullCols = df
        .select(
          row.schema.fieldNames
            .intersect(colKeep)
            .map(c => col(c.drop(4).dropRight(1))): _*
        )
      delaysNoNullCols
    }

    /**
     * Spark has this capability built-in when reading also with option flags
     *
     * @param columns
     *   from df
     * @return
     *   column values after applying conversion to null type
     */
    def na2Null(columns: Array[String], nullStr: String): Array[Column] =
      columns.map(c => when(col(c) === nullStr, null).otherwise(col(c)).alias(c))

    /**
     * This applies the fix for nulls as well as casting of all types using our seq of strings
     * defined above
     *
     * @param df
     * @return
     *   dataframe with types casted and fixed nulls
     */
    def fixNullAndTypes(df: DataFrame, types: Seq[(String, String)]): DataFrame = df
      .select(na2Null(df.columns, "NA"): _*)
      .select(types.map { case (c, t) => col(c).cast(t) }: _*)

    /**
     * Function to add some convenience columns to show the day of the week during analysis Note
     * that mon = 1 for airlines
     *
     * @param col
     * @return
     */
    def dayOfWeekStr(col: Column): Column = when(col === lit(1), lit("Mon"))
      .when(col === lit(2), lit("Tue"))
      .when(col === lit(3), lit("Wed"))
      .when(col === lit(4), lit("Thu"))
      .when(col === lit(5), lit("Fri"))
      .when(col === lit(6), lit("Sat"))
      .when(col === lit(7), lit("Sun"))

    val airportCodesPath      = "s3a://datasets-rk/airport-codes.txt"
    val baseFilePath          = "s3a://datasets-rk/airlines"
    val airlineFileHeaderPath = Seq(s"$baseFilePath/flight_1987.csv")
    // val filesAfterFirst  = Seq(s"$baseFilePath/flight_1988.csv")
    // val smallfile = Seq("s3a://datasets-rk/flight_2008_small.csv")
    val airlineFilesNoHeaderPath =
      Seq(s"$baseFilePath/flight_1988.csv", s"$baseFilePath/flight_1989.csv")

    /**
     * Add city names by joining with airport code dataset
     *
     * @param delays
     * @param codes
     * @return
     */
    def addCity(delays: DataFrame, codes: DataFrame) = delays
      .join(codes, delays("origin") === codes("iata"), "left")
      .withColumnRenamed("city", "origcity")
      .drop("iata")
      .join(codes, delays("dest") === codes("iata"), "left")
      .withColumnRenamed("city", "destcity")
      .drop("iata")

    /**
     * We could have also decidid to use a case class with StructTypes to then use a Dataset instead
     */
    val airlineTypes: Seq[(String, String)] = Seq(
      ("actualelapsedtime", "string"),
      ("airtime", "integer"),
      ("arrdelay", "integer"),
      ("arrtime", "integer"),
      ("cancellationcode", "string"),
      ("cancelled", "integer"),
      ("carrierdelay", "integer"),
      ("crsarrtime", "integer"),
      ("crsdeptime", "integer"),
      ("crselapsedtime", "integer"),
      ("date", "date"),
      ("day", "string"),
      ("dayofmonth", "integer"),
      ("dayofweek", "integer"),
      ("depdelay", "integer"),
      ("deptime", "integer"),
      ("dest", "string"),
      ("destcity", "string"),
      ("distance", "integer"),
      ("diverted", "integer"),
      ("flightnum", "integer"),
      ("isarrdelay", "integer"),
      ("isdepdelay", "integer"),
      ("lateaircraftdelay", "integer"),
      ("month", "integer"),
      ("nasdelay", "integer"),
      ("origcity", "string"),
      ("origin", "string"),
      ("route", "string"),
      ("securitydelay", "integer"),
      ("tailnum", "integer"),
      ("taxiin", "integer"),
      ("taxiout", "integer"),
      ("uniquecarrier", "string"),
      ("weatherdelay", "integer"),
      ("year", "integer")
    )
    def getAirportCodes(path: Seq[String]) = readCsv(
      path,
      hasHeader = true,
      inferSchema = false,
      delim = "\t"
    ).get
      .transform(cleanHeader())
      .na
      .drop(Seq("iata"))
      .select("iata", "city")

    // some udfs to add booleans I originally removed to create my own instead
    // notice how we can call these variables in the lambdas pretty much anything
    val isDelayedArr = udf((arrTime: String, schedArrTime: String) => arrTime > schedArrTime)
    val isDelayedDep = udf((depTime: String, schedDepTime: String) => depTime > schedDepTime)

    /**
     * **Part A:** Airlines Question 1 [5 Points]
     *   - Write code that uses the DataFrame API to read in the entire airlines data set with
     *     clearly named columns.
     */

    /**
     * This will get all of the airline data and use the header from the first file and then joining
     * the sets with a union Alse runs a cleanser to standardize column names Also joins with
     * airport code data to add city information. We are also taking advantage of taking the chance
     * to create a nicer date attribute
     *   - Adding a day of week string that maps to the weekday numbers for airlines
     *   - adding a route attribute since that might be an useful datapoint to aggregate on
     *   - adding some boolean values for whether or not a departure ar arrival was delayed.
     * @param dfWithHeader:
     *   df with header info
     * @param filepaths:
     *   filepaths to all files without schema
     * @return
     */
    def getAirlines(dfWithHeader: Option[DataFrame], filepaths: Seq[String]) = readCsv(
      filepaths,
      inferSchema = false,
      hasHeader = false,
      schema = Some(dfWithHeader.get.schema)
    ).get
      .union(dfWithHeader.get)
      .transform(cleanHeader())

    def initAirlines(
      delays: DataFrame,
      codes: DataFrame,
      types: Seq[(String, String)]
    ) = dropAllNACols(
      fixNullAndTypes(
        addCity(delays, codes)
          .withColumn("date", expr("make_date(year, month, dayofmonth)"))
          .withColumn("day", dayOfWeekStr(col("dayofweek")))
          .withColumn("route", (col("origin") + "-" + col("dest")))
          .withColumn(
            "isarrdelay",
            when(isDelayedArr(col("arrtime"), col("crsarrtime")) === true, 1).otherwise(0)
          )
          .withColumn(
            "isdepdelay",
            when(isDelayedDep(col("deptime"), col("crsdeptime")) === true, 1).otherwise(0)
          )
          .transform(cleanHeader()),
        types
      )
    )

    val airportCodes = getAirportCodes(Seq(airportCodesPath))
    val airlines: DataFrame =
      getAirlines(readCsv(airlineFileHeaderPath, inferSchema = false), airlineFilesNoHeaderPath)
        .limit(1000)

    // println(airlines.rdd.getNumPartitions)
    val delays = initAirlines(airlines, airportCodes, airlineTypes)
    delays.show(false)

    /**
     * **Part B:** Airlines Question 2 [5 Points]
     *   - How many unique airlines are present in this dataset?
     */
    println(delays.count()) // total records

    // lets take a peek at the values we have in there
    getDistinctVals(delays.na.drop.select("uniquecarrier")).show
    println(delays.na.drop(Seq("uniquecarrier")).select("uniquecarrier").distinct().count())

    // another method using group by can be useful as well
    delays.groupBy($"uniquecarrier").count().show()

    // if speed matters more
    // import org.apache.spark.sql.functions.approx_count_distinct
    delays.na
      .drop(Seq("uniquecarrier"))
      .agg(approx_count_distinct("uniquecarrier").as("distinct_arr"))
    // .show()

    // using .isNotNull Method
    println(
      delays.filter(col("uniquecarrier").isNotNull).select("uniquecarrier").distinct().count()
    )

    println(delays.dropDuplicates("uniquecarrier").distinct().count())

    /**
     * **Part C:** Airlines Question 3 [10 Points]
     *   - Which airline is delayed on departure most often? Show a bar graph of the top five most
     *     delayed airlines. Since we already have a depdelay boolean we can work with lets filter
     *     first and then group by to see which airline has the most instances of 1 in terms of
     *     frequency
     */
    delays.filter($"depdelay" === 1).groupBy("uniquecarrier").count.orderBy(desc("count")).show(5)

    /**
     * **Part D:** Airlines Question 4 [15 Points]
     *   - Part a: What was the average arrival delay per airline?
     *   - Part b: Also, for each airline, on average did flights arrive early or late?
     *   - Part c: Add a column to this new dataframe (containing the grouped averages) that
     *     contains the string "Late" if the average arrival for that airline arrive >15 minutes
     *     late, "Early" if the average arrival is <0 minutes late, and "On-time" if the average
     *     arrival is between 0 and 15 minutes late.
     *
     * Important -> To add the additional column, use a Spark UDF. Additionally, make sure to filter
     * out or fill in null values in your dataframe (if there are any) prior to applying the UDF.
     */

    // part a: What was the average arrival delay per airline?
    delays
      .groupBy("uniquecarrier")
      .agg(avg("arrdelay").as("avgdelay"))
      .orderBy(desc("avgdelay"))
    // .show

    // part b: Also, for each airline, on average did flights arrive early or late?
    delays
      .groupBy("uniquecarrier")
      .agg(avg("isarrdelay").alias("avg_isdelay"))
      .withColumn("avg_early_or_late", when(col("avg_isdelay") > 0.5, "late").otherwise("early"))
    // .show

    // part c: Add a column to this new dataframe (containing the grouped averages) that

    /**
     * Add late/early/ontime strings using a pattern match and a udf function
     *
     * @return
     */
    def howIsMyTiming = udf((avgdelay: Double) =>
      avgdelay match {
        case x: Double if x > 15          => "Late"
        case x: Double if x < 0           => "Early"
        case x: Double if x > 0 && x < 15 => "On-Time"
      }
    )

    delays
      .filter(col("uniquecarrier").isNotNull && col("arrdelay").isNotNull)
      .groupBy("uniquecarrier")
      .agg(avg("arrdelay").alias("avgdelay"))
      .withColumn("avg_early_or_late", howIsMyTiming(col("avgdelay")))
    // .show(false)

    // non-udf version
    // .orderBy(desc("avg_delay"))
    // .withColumn(
    //   "is_early_late_ontime",
    //   when(col("avg_delay") > 15, "Late")
    //     .when(col("avg_delay") < 0, "Early")
    //     .when(col("avg_delay") > 0 && col("avg_delay") < 15, "on-time")
    // )

    // val spec = Window.partitionBy("date", "origcity")
    // delays
    //   .filter("isdepdelay = 1 and cancelled = 0")
    //   .select(
    //     "date",
    //     "origcity",
    //     "uniquecarrier",
    //     "flightnum",
    //     "crsdeptime",
    //     "isdepdelay",
    //     "depdelay"
    //   )
    //   .withColumn("depdelmin", min("depdelay").over(spec))
    //   .withColumn("depdelmax", max("depdelay").over(spec))
    //   .withColumn("depdelavg", round(avg("depdelay").over(spec), 2))
    //   .withColumn("depdelsum", sum("depdelay").over(spec))
    //   .orderBy("date", "origcity", "depdelay")
    //   .show

    /**
     * **Part E:** Airlines Question 5 [15 Points]
     *   - What file format is airlines data stored in, and was this the most optimal format for the
     *     questions asked above?
     *   - What format would you store this data in if you frequently queried only the UniqueCarr
     *     and CancellationCode columns?
     *   - What if you frequently read entire rows of the dataset?
     *
     * Note: Cite any sources used. You do not need a code answer for this question.
     *
     * **Part F:** Airlines Question 6 [5 Points]
     *   - If you needed to keep multiple versions of this dataset, why might you use the Delta
     *     format to do this efficiently?
     */

    println(
      """
      Airlines dataset is stored in CSV format with a total of 16 different
      files located in DBFS. There are no partitions within the datasets and
      the first file is the only one containing header information. In general
      it's recommended to store data in parquet format in order to take
      advantage of several important benefits such as partition pruning.
      metadata statistics and projection pruning to create filters and subsets
      of data as close as possible to the actual data. Parquet is a very
      powerful self-describin columnar format highly suitable for analytical
      queries and allows for performance benefits derived from data skipping.
      Partitioning becomes especially important when common transformations
      such  joins require the shuffling of data to be co-located and moved
      across the network. This particulay dataset isn't terribly large and
      although uncompressed csv is not optimal it was sufficient for the
      particular aggregations we performed. If there is a requirement to allow
      analysts an easier mechanism to access tables then various
      transformations can also be saved in the spark hive metastore with the
      `.saveAsTable` command to save to persistent tables. Generally speaking
      date tends to be a good choice for partitioning saved tables. If on the
      other hand carrier and cancellation code are often used as queries then
      since they both have low cardinality, I would not necessarily recommend
      saving the data with those columns or even creating double nested
      partition since performance should not be an issue. One can of course
      save a table in delta partitioned with carrier as follows which might be
      a good choice for analysis as well. Again, this would only be in the
      event that very large data would benefit from partition pruning.
  
      df.write.format("parquet")
        .partitionBy("uniquecarrier")
        .option("path", "<pathtocarrier>")
        .saveAsTable("carriers")

    If all rows are required to be read then I would recommend that filter
    predicates and pushdowns are used up front as much as possible to try and
    minimize memory footprint. Applying thoughtful column and row pruning
    during reading followed by the careful and strategic useage of caching
    certain aggergations and filters can be a good way to minimize memory
    footprint.

    One of the many benefits of the delta format is spark is the ability to
    leverage "time travel." Every time you write into delta table every
    operation is is automatically versioned allowing you to query an older
    snapshot. Accessing snapshots is achieved by either version number of
    timestamp
    https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html


      """
    )

    spark.close()
  }

}
