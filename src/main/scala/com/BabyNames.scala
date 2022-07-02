package com

import com.databricks.spark.xml.functions.from_xml
import com.databricks.spark.xml.schema_of_xml
import org.apache.spark.ml.feature._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, when}
import org.apache.spark.sql.types._

import functions._

case class Names(
  sid: String,
  id: String,
  position: String,
  created_meta: String,
  updated_meta: String,
  meta: String,
  year: String,
  first_name: String,
  county: String,
  sex: String,
  count: Int,
  created_at_ts: java.sql.Timestamp,
  updated_at_ts: java.sql.Timestamp)

object BabyNames {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("babynames")
      .config("spark.debug.maxToStringFields", "5000")
      .getOrCreate()

    // these keys are exported as gpg env keys with my sessions
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.access.key", sys.env.get("AWS_ACCESS_KEY_ID").get)
    spark.sparkContext.hadoopConfiguration
      .set("fs.s3a.secret.key", sys.env.get("AWS_SECRET_ACCESS_KEY").get)

    def getDistinctVals(df: DataFrame): DataFrame =
      df.select(df.columns.map(c => collect_set(col(c)).alias(c)): _*)

    /**
     * Cleans dataframe header by applying a regex and converting to upper case along with replacing
     * spacing with underscore.
     *
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
     * helper to convert a string to nulls manually. Spark can do this natively when reading.
     *
     * @param columns
     * @return
     */
    def na2Null(columns: Array[String], str: String): Array[Column] =
      columns.map(c => when(col(c) === str, null).otherwise(col(c)).alias(c))

    /**
     * A string cleaner UDF
     *
     * @return
     */
    def cleanChars = udf((str: String) =>
      str
        .toLowerCase()
        .replace(" ", "")
        .replaceAll("[\\.$|,|;|'|\\s|#]", "")
    )

    import spark.implicits._

    /**
     * Start BabyNames Processing
     */
    val babyVisitFile: String = "s3a://datasets-rk/babynames/births-with-visitor-data.json"
    // val babyNamesFile: String = "s3a://datasets-rk/babynames/babyname-small.json"
    val babyNamesFile: String = "s3a://datasets-rk/babynames/babyname-full.json"

    // ------------------------------------------------------------------//
    //                  Part 1 Without Any XML Parsing                  //
    // ------------------------------------------------------------------//

    /**
     * Q1: Baby Names Question 1 - Nested Data [15 Points] Use Spark SQL's native JSON support to
     * read the baby names file into a dataframe. Use this dataframe to create a temporary table
     * containing all the nested data columns ("sid", "id", "position", "created_at",
     * "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex",
     * "count") so that they can be queried using SQL. Please provide your brief, written
     * description of your code here...
     *
     * Hint: you can use dbutils.fs.head(baby_names_path) to take a look at the dataset before
     * reading it in.
     *
     * Suggested Steps:
     *
     * Read in the JSON data Pull all columns in the nested data column to top level, following the
     * schema specified above. There are built-in Spark SQL functions that will accomplish this.
     * Create a temp table from this expanded dataframe using createOrReplaceTempView()
     *
     * @param df
     * @return
     */

    /**
     * After running the `fs.head` command it looks like we need to deal with an upper metadata
     * portion of the json file. We can approach this initial extraction using any of the available
     * APIs including native sql by creating a `TEMPORARY` view as well as the `spark.read.json...`
     * which we will also demonstrate for the sake of completeness. Since we are dealing with a 1
     * level deep json with 2 sub-root levels we can explode on the `data` field containing all of
     * the data we need for subsequent processing and analysis. In the first scenario prior to any
     * XML injection or integration we can start with a simple sql function reading the json file as
     * is followed by selection the various attributes with aliasing. I also make sure to
     * pro-actively convert the unix epoch attributes to date formats and also run a simple string
     * cleanser for the attributes that we will be using for aggregations and I personally like to
     * create new cols representing the cleaned columns while still mantaining the original columns
     * in the dataset for final outputs
     */

    /**
     * This will read the babyname data file by using the native spark sql json reader synax and
     * create a temporary view we will query against
     *
     * Important note here is the usage of the `explode` method on our `data` column in order to
     * access the second level within the json file after the meta portion. Explode is a great
     * method to work with complex data types in the form of Struct and Arrays after flattening Our
     * original json data consists of multiple lines of self contained arrays within the `data` key
     * that looks as follows:
     *
     * "data": [ [ "row-r9pv-p86t.ifsp", "00000000-0000-0000-0838-60C2FFCC43AE", 0, 1574264158,
     * null, 1574264158, null, "{ }", "2007", "ZOEY", "KINGS", "F", "11" ],...[next object...]
     *
     * We need to access the data within this data key by using explode In this step we are also
     * accessing the `data` attribute along with the location within that key followed by aliasing
     * those individual values
     *
     * We are also taking the chance here to just create some nicer representations of the ts
     * columns with a simple cast and then dropping the original ts attributes. Of course, being
     * mindful that downsteanm sinks may require the original unix epoch formats therefore we can
     * either convert back or not drop the originals.
     *
     * In the final step here we are creating our pre-processed view called `babyview_data`
     *
     * @param Parameterless
     *   function
     */
    def explodeBabyViewSQL() = {
      spark
        .sql(
          "CREATE TEMPORARY VIEW babyview USING json OPTIONS" + // <-- This is the native sql approach
            s" (path '$babyNamesFile', multiline=true)"
        )
      spark
        .sql("select * from babyview") // <-- We can now run sql against the view we created above
        .select(explode(col("data")).as("data"))
        .select(
          $"data" (0).as("sid"),
          $"data" (1).as("id"),
          $"data" (2).as("position"),
          $"data" (3).as("created_at"),
          $"data" (4).as("created_meta"),
          $"data" (5).as("updated_at"),
          $"data" (6).as("updated_meta"),
          $"data" (7).as("meta"),
          $"data" (8).as("year"),
          $"data" (9).as("first_name"),
          $"data" (10).as("county"),
          $"data" (11).as("sex"),
          $"data" (12).as("count").cast(IntegerType)
        )
        .withColumn("created_at_ts", from_unixtime(col("created_at")).cast(TimestampType))
        .withColumn("updated_at_ts", from_unixtime(col("updated_at")).cast(TimestampType))
        .withColumn("county_clean", cleanChars(col("county")))
        .withColumn("first_name_clean", cleanChars(col("first_name")))
        .drop("created_at", "updated_at")
        .createOrReplaceTempView("babyview_data") // <---- Our final view for analysis.
    }

    explodeBabyViewSQL() // this will create oukr virtual table to run sql against
    // spark.sql("""select * from babyview_data""").show(false)
    // spark.sql("""select * from babyview_data""").printSchema()

    /**
     * Q2: Baby Names Question 2 - Multiple Languages [10 Points] Using the temp table you created
     * in the question above, write a SQL query that gives the most popular baby name for each year
     * in the dataset. Then, write the same query using either the Scala or Python dataframe APIs.
     *
     * Please provide your brief, written description of your code here...
     */

    /**
     * "At its core, a window function calculates a return value for every input row of a table
     * based on a group of rows, called the Frame. Every input row can have a unique frame
     * associated with it. This characteristic of window functions makes them more powerful than
     * other functions and allows users to express various data processing tasks that are hard (if
     * not impossible) to be expressed without window functions in a concise way. Now, let’s take a
     * look at two examples." Source:
     * https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
     *
     * The below sql statement is a demonstration of a the rank window function using a purely spark
     * sql based approach. Also known as a "Window Function" the idea of a rank window is that rows
     * within a partition which in this case is year that have the same value will receive the same
     * rank with the first rank in the partition being 1. The order by clause is important since it
     * specifies the order within each partition the function is applied to. Really great reference
     * for rank versus dense rank:
     * https://towardsdatascience.com/how-to-use-sql-rank-and-dense-rank-functions-7c3ebf84b4e8
     */

    /**
     * Run a window rank function to get the most popular names by year. We could also have chosen
     * to use a dense_rank here if we cared about non-skipping ranks
     *
     * The name "Daniel" is crearly a winner in many of the years.
     *
     * @return
     */
    def mostPopularNameByYearSQL(): DataFrame = spark
      .sql("""SELECT
        first_name_clean,
        count,
        year
        FROM (
          SELECT
          first_name_clean,
          count,
          year,
          rank() OVER (PARTITION BY year ORDER BY count DESC) as rank
          FROM babyview_data
        ) tmp
      WHERE rank == 1 ORDER BY year""")

    // mostPopularNameByYearSQL().show(false)

    /**
     * This demonstrates using the exact same `.select(explode)...but against the dataframe argument
     * supplied as opposed to a table view. Since count will be an important attribute for the
     * aggregations we also want to make sure that we are appropriately casting the count column to
     * an integer type.
     *
     * @param df:
     *   raw json dataframe after reading before any processing
     * @return:
     *   dataframe with exploded data and other cleansing performed
     */
    def getBabyNameJson(filepath: String): DataFrame = spark.read
      .option("multiLine", true)
      .json(filepath)

    def explodeBabyDF(df: DataFrame): DataFrame = df
      .select(explode($"data").as("data"))
      .select(
        $"data" (0).as("sid"),
        $"data" (1).as("id"),
        $"data" (2).as("position"),
        $"data" (3).as("created_at"),
        $"data" (4).as("created_meta"),
        $"data" (5).as("updated_at"),
        $"data" (6).as("updated_meta"),
        $"data" (7).as("meta"),
        $"data" (8).as("year"),
        $"data" (9).as("first_name"),
        $"data" (10).as("county"),
        $"data" (11).as("sex"),
        $"data" (12).as("count").cast(IntegerType)
      )
      .withColumn("created_at_ts", from_unixtime(col("created_at")).cast(TimestampType))
      .withColumn("updated_at_ts", from_unixtime(col("updated_at")).cast(TimestampType))
      .withColumn("county_clean", cleanChars(col("county")))
      .withColumn("first_name_clean", cleanChars(col("first_name")))
      .drop("created_at", "updated_at")

    /**
     * we can then create our cleaned up baby name data by composing a couple of HOFs in order to
     * retrieve and prep the json data set. Note that we are also taking advantage of a case class
     * and dataset encoding in this scenario to potential take advantage of compile-time type safety
     * while being mindful of potential performance issues from tungsten and serializations that
     * need to take place since conversion needs to take place between JVM objects and tables
     */
    // explodeBabyDF(getBabyNameJson(babyNamesFile)).as[Names].show(false)
    // explodeBabyDF(getBabyNameJson(babyNamesFile)).as[Names].printSchema()

    /**
     * Q2 Part 2: Using a Window rank function with a Dataset[T] as an input followed by a groupBy,
     * we can easily view the top names by county and we could also easily have returned a dataset
     * by pre-defining a schema with another case class and using the `.as[T]` method. Reference:
     * https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html Takes a
     * dataset of babynames that has already been pre-processed with some cleansing and theh uses a
     * Window.partitionBy to create a ranking of names by year and county ordeing by count
     *
     * The expected resuld here should of course match up exactly to our previous SQL query.
     *
     * @param ds
     * @return
     *   dataframe with the top names by county and year
     */
    def mostPopularNamesByYearScala(ds: Dataset[Names]): DataFrame = ds
      .withColumn("rnk", row_number().over(Window.partitionBy("year").orderBy(col("count").desc)))
      .where(col("rnk") === 1)
      .orderBy(asc("year"))
      .select("first_name_clean", "count", "year")

    // val resMostPopularNameScala = mostPopularNamesByYearScala(
    //   explodeBabyDF(getBabyNameJson(babyNamesFile)).as[Names]
    // )
    // resMostPopularNameScala.show()
    // val resMostPopularNameSQL = mostPopularNameByYearSQL()
    // resMostPopularNameSQL.show()
    //
    // // quick equality check...
    // resMostPopularNameSQL.except(resMostPopularNameScala).show()

    // we can choose to cast our dataframe generated from babyview_data view to
    // our dataset and then use that as our input
    // mostPopularNamesByYearScala(spark.sql("select * from babyview_data").as[Names]).show(false)

    // We can also bypass spark sql and simply use oun json.read method as input
    // val topBabyNamesByCountyYear =
    //   mostPopularNamesByYearScala(explodeData(getBabyNameJson(babyNamesFile)).as[Names])
    // topBabyNamesByCountyYear.show(false)

    /**
     * Q3: Baby Names Question 3 - Performance [10 Points] Are there any performance considerations
     * when choosing a language API (SQL vs Python vs Scala) in the context of Spark?
     *
     * Are there any performance considerations when using different data representations (RDD,
     * Dataframe) in Spark? Please explain, and provide references if possible. No code answer is
     * required.
     */

    // println(
    //   """
    //   Broadly speaking, one of the biggest factors effecting performance in
    //   spark tends to be when data has to be moved around over the network for
    //   shuffling purposes. Essentially, co-located data will always be faster
    //   than data that must first be moved and re-shuffled. "Shuffling" is
    //   literally a physical movement mechanism. The realiy is that common
    //   transforamtions such as join, groupBy, reduceBy, repartition, and
    //   distinct will always result in a reshuffle. Spark does however have the
    //   capability to avoid shuffling whenever a previous transformation has
    //   already partitioned the data into a single partition. It's also a good
    //   idea to always cache your dataframes when frequent subsequent iterations
    //   are needed but I do tend to use `.cache()` quite conservatively to avoid
    //   any spilling to disk. With regards to API performance implications...UDFs
    //   with scala or python can have negative serialization cost effects whereas
    //   using the natively supported transformations API from either scala or
    //   python or sql would have similar performance. Native transformations
    //   create a query plan under the hood and the plan will be the same
    //   regardless of the used language. \"The native approach with HOFs is the
    //   most efficient — it is not surprising, it can leverage all the internal
    //   features such as Spark optimizer, code generation, or internal Tungsten
    //   data format.\"
    //   --https://towardsdatascience.com/performance-in-apache-spark-benchmark-9-different-techniques-955d3cc93266
    //
    //   With respect to RDD versus DataFrames, spark does not use the catalyst
    //   optimizer against RDDs and furthermore, the RDD API cannot take advantage
    //   of wholestage codegen or the tungsten data format
    //
    //   "Spark 1.3 introduced a new DataFrame API as part of the Project Tungsten
    //   initiative which seeks to improve the performance and scalability of
    //   Spark. The DataFrame API introduces the concept of a schema to describe
    //   the data, allowing Spark to manage the schema and only pass data between
    //   nodes, in a much more efficient way than using Java serialization. The
    //   DataFrame API is radically different from the RDD API because it is an
    //   API for building a relational query plan that Spark’s Catalyst optimizer
    //   can then execute. The API is natural for developers who are familiar with
    //   building query plans\"
    //   --https://stackoverflow.com/questions/31508083/difference-between-dataframe-dataset-and-rdd-in-spark
    //   """
    // )

//------------------------------------------------------------------//
//                          Part 2: XML Injection Processing        //
//------------------------------------------------------------------//
    /**
     * Q4: #### Baby Names Question 4 - Nested XML [15 Points] Imagine that a new upstream system
     * now automatically adds an XML field to the JSON baby dataset. The added field is called
     * visitors. It contains an XML string with visitor information for a given birth. We have
     * simulated this upstream system by creating another JSON file with the additional field.
     *
     * Using the JSON dataset at dbfs:/interview-datasets/sa/births/births-with-visitor-data.json,
     * do the following:
     *
     * **PART A**: Read the births-with-visitor-data.json file into a dataframe and parse the nested
     * XML fields into columns and print the total record count.
     *
     * **PART B**: Find the county with the highest average number of visitors across all births in
     * that county
     *
     * **PART C**: Find the average visitor age for a birth in the county of KINGS
     *
     * **PART D**: Find the most common birth visitor age in the county of KINGS
     *
     * visitors_path = "/interview-datasets/sa/births/births-with-visitor-data.json"
     *
     * ## Hint: the code below will read in the downloaded JSON files. However, the xml column needs
     * to be given structure. Consider using a UDF. #df = spark.read.option("inferSchema",
     * True).json(visitors_path)
     */

    /**
     * **PART A**: Read the births-with-visitor-data.json file into a dataframe and parse the nested
     * XML fields into columns and print the total record count.
     *
     * Initialize and prepare the baby names dataset containing the XML injected record requiring
     * parsing. After reading the data with the built-in spark json reader with multyline false, we
     * simply use the `from_xml` method to parse the "visitors" attribute containing the birth
     * visits data. We also take the chance to add some columns here such as the count of visitors,
     * by simply using the `size` method, a column for ages extracted and flattened from the xml
     * record as well as cleaned up county and name values for the analytics using a small UDF.
     *
     * @param df
     *   from the original json file
     * @return
     *   a dataframe with a parsed xml field and some cleaned values and new columns for downstream
     *   processing
     *
     * new columns:
     *   - birth_visits: the full "visitors" data from the xml record
     *   - num_visitors: the number of visitors for a given birth
     *   - ages: a column containing only the ages extracted from the xml record to make it easier
     *     to access and run aggregations against
     *   - county_clean: a cleaned up county value
     *   - name_clean: a cleaned up name value
     *
     * We also drop any rows that have nulls in our county col attribute
     *
     * I tend to composing a bunch of HOF for datafram transformations but I do prefer to instead
     * use dataset `.transform` method for a cleaner look ie transforamations: def transform[U](t:
     * Dataset[T] => Dataset[U]): Dataset[U] = t(this)
     */
    def getBabyVisitJsonXML(filepath: String): DataFrame = spark.read
      .option("multiLine", false) // <-- we have to use default setting here
      .json(filepath)

    /**
     * Funtion to initialize and pre-process baby name data containing xml injected records
     *
     * @param df
     *   from original json file
     * @return
     *   a dataframe with a parsed xml field and some cleaned values and new columns for downstream
     */
    def initBabyNameXML(df: DataFrame): DataFrame = df
      .withColumn(
        "birth_visits",
        from_xml($"visitors", schema_of_xml(df.select("visitors").as[String])).getItem("visitor")
      )
      .withColumn("num_visitors", size(col("birth_visits")))
      .withColumn("ages", flatten(array_repeat($"birth_visits._age", 1)))
      .withColumn("county_clean", cleanChars(col("county")))
      .withColumn("firstname_clean", cleanChars(col("first_name")))
      .na
      .drop(Seq("county_clean"))
      .drop("visitors")

    println(initBabyNameXML(getBabyVisitJsonXML(babyVisitFile)).count())
    initBabyNameXML(getBabyVisitJsonXML(babyVisitFile)).printSchema()
    initBabyNameXML(getBabyVisitJsonXML(babyVisitFile)).show(false)

    /**
     * **PART B**: Find the county with the highest average number of visitors across all births in
     * that county
     *
     * We can groupby county and run an aggregation with avg and then sort by desc
     *
     * @param df
     *   our pre-processed dataframe
     * @return
     *   the aggregation showing the counties with the highest average number of visitors
     */
    def mostVisitsByCounty(df: DataFrame): DataFrame = df
      .groupBy("county_clean")
      .agg(avg("num_visitors").alias("avg_num_visitors"))
      .orderBy(desc("avg_num_visitors"))
    mostVisitsByCounty(initBabyNameXML(getBabyVisitJsonXML(babyVisitFile))).limit(1).show(false)

    /**
     * **PART C**: Find the average visitor age for a birth in the county of KINGS that county
     *
     * Aggregation and custom Scala UDF to find the avg visitor age for births across counties
     * Methodology: We use a sum age udf to create a new column and then simply divide that by our
     * num_visitors column to get avg ages
     *
     * @param df
     *   the pre-processed dataframe
     * @return
     *   singe row from groupby after filtering for kings county
     */
    def getAvgAgeVisitWithUDF(df: DataFrame): DataFrame = {
      val sumAge = udf((xs: Seq[Row]) => xs.map(_.getAs[Long]("_age")).sum)
      df
        .withColumn("avg_age", sumAge(col("birth_visits")) / col("num_visitors"))
        .drop("birth_visits")
        .groupBy("county_clean")
        .agg(avg("avg_age").alias("avg_age"))
        .filter(col("county_clean") === "kings")
    }
    getAvgAgeVisitWithUDF(initBabyNameXML(getBabyVisitJsonXML(babyVisitFile))).show(false)

    /**
     * **PART D**: Find the most common birth visitor age in the county of KINGS
     *
     * Aggregation to find the most common visits age by county. Uses `explode` function from spark
     * in order to retrieve the ages values from out ages column we initially created and then
     * groups by county and age.
     *
     * The second step to the calculation involves a window function partitioned by county and
     * finally listing out the most common age visits by only looking at the ranks that have 1 for
     * each county partition.
     *
     * @param df
     *   pre-processed dataframe
     * @return
     *   single row from groupby after filtering for kings county
     */
    def mostCommonAgeVisits(df: DataFrame): DataFrame = {
      val explodeAgeAndGrouped = df
        .select(col("county_clean"), explode(col("ages")).alias("age"))
        .groupBy("county_clean", "age")
        .count()
      explodeAgeAndGrouped
        .withColumn(
          "rank",
          row_number().over(Window.partitionBy("county_clean").orderBy(desc("count")))
        )
        .where(col("rank") === 1)
        .orderBy(desc("count"))
        .drop("rank")
        .filter(col("county_clean") === "kings")
    }
    mostCommonAgeVisits(initBabyNameXML(getBabyVisitJsonXML(babyVisitFile))).show(false)

    // ------------------------------------------------------------------//
    //           Appendix for things I was curious to try out           //
    // ------------------------------------------------------------------//
    def addAgeCols(df: DataFrame): DataFrame = {
      val numCols = df
        .withColumn("_num_visitors", size($"birth_visits"))
        .agg(max($"_num_visitors"))
        .head()
        .getInt(0)
      df
        .select(
          df.columns.map(col) ++ (0 until numCols).map(i =>
            $"birth_visits._age".getItem(i).as(s"age$i")
          ): _*
        )
    }
    // addAgeCols(dataPrep(getBabyVisitJsonXML(birthsVisitFile))).show(false)

    // another interesting way to get avg age column is to use fold left and to make
    // sure catalyst optimizer does not have appriciably different performance
    def getAvgAgeVisitFoldLeft(df: DataFrame): DataFrame = {
      val totalAge = (0 to df.agg(max(size($"birth_visits"))).as[Int].first)
        .map(i => coalesce($"birth_visits._age".getItem(i), lit(0)))
        .foldLeft(lit(0))(_ + _)
      df.withColumn("avg_age", totalAge / col("num_visitors"))
    }
    // getAvgAgeVisitFoldLeft(dataPrep(getBabyVisitJsonXML(babyVisitFile))).show(false)

    spark.close()
  }

}
