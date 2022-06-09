import scala.util.Random._
import org.apache.spark.sql._
import org.apache.spark.Partition
import org.apache.spark.sql.functions._

val spark = SparkSession.builder
  .appName("SparkSQL")
  .master("local[4]") //only for local mode
  .getOrCreate()

import spark.implicits._

val ds = Seq(
  Array("123", "abc", "2017", "ABC"),
  Array("456", "def", "2001", "ABC"),
  Array("789", "ghi", "2017", "DEF")
).toDF("col")

ds.show(false)

ds.withColumn("col1", element_at('col, 1))
  .withColumn("col2", element_at('col, 2))
  .withColumn("col3", element_at('col, 3))
  .withColumn("col4", element_at('col, 4))
  .drop('col)
  .show()

val str0 = "dbfs:/interview-datasets/data/part-"
val ids: IndexedSeq[String] = (1 until 16).map(i => f"$i%05d")
val fileLst = (1 until 16).map(i => str0 + f"$i%05d".toString()).toSeq

def printConfigs(session: SparkSession) = {
  // Get Conf
  val mconf = session.conf.getAll
  for (k <- mconf.keySet) println(s"{k} -> ${mconf(k)}")
}

printConfigs(spark)

val sqlCfgs = spark.sparkContext.getConf.getAll
for (v <- sqlCfgs) println(v)

// case class Activity(userId: String, cartId: String, itemId: String)
//
// val activities = Seq(
//   Activity("u1", "c1", "i1"),
//   Activity("u1", "c2", "i2"),
//   Activity("u1", "c3", "i1"),
//   Activity("u1", "c4", "i3"),
//   Activity("u1", "c5", "i3")
// )
//
// val df = spark
//   .createDataFrame(activities)
//   .toDF("user:id", "cart_id", "item_id")
//
// df.show()
//
// case class Store(
//   name: String,
//   capacity: Int,
//   opens: Int,
//   closes: Int)
//
// val stores = Seq(
//   Store("a", 24, 8, 20),
//   Store("b", 36, 7, 21),
//   Store("c", 18, 5, 23)
// )
//
// val storesDF = spark.createDataFrame(stores)
// storesDF.createOrReplaceTempView("stores")
// storesDF.show
//
// val q = spark.sql("select * from stores where closes >= 22")
// q.show()
//
// import org.apache.spark.sql.functions._
//
// storesDF.where(storesDF("closes") >= 22).show()
// storesDF.where(col("closes") >= 22).show()
// storesDF.where($"closes" >= 22).show()
//
// storesDF
//   .select("name")
//   .where('capacity > 20)
//   .show
//
// case class StoreOccupants(storename: String, occupants: Int)
//
// val occupants = Seq(
//   StoreOccupants("a", 8),
//   StoreOccupants("b", 20),
//   StoreOccupants("c", 16),
//   StoreOccupants("d", 55),
//   StoreOccupants("e", 8)
// )
//
// val occupancy = spark.createDataFrame(occupants)
// occupancy.createOrReplaceTempView("occupancy")
// occupancy.show()
//
// /**
//  * Inner, Right, Left, Semi, Anti, Full
//  */
//
// // Inner Join
// val innerJoin = storesDF
//   .join(occupancy)
//   .where(storesDF("name") === occupancy("storename"))
// innerJoin.show()
//
// val rightJoin = storesDF
//   .join(occupancy, storesDF("name") === occupancy("storename"), "right")
// rightJoin.show()
//
// val leftJoin = storesDF
//   .join(occupancy, storesDF("name") === occupancy("storename"), "left")
// leftJoin.show()
//
// // coffee shops with < 20 capacity
// val lessThan20 = storesDF
//   .join(occupancy, storesDF("name") === occupancy("storename"), "left")
//   .where(col("capacity") < 20)
// lessThan20.show()
//
// spark.sql("""SELECT stores.`name` FROM stores where capacity < 20""").show()
//
// val parallelRDD = spark.sparkContext.parallelize(1 to 9)
//
// // these keys are accessed after sourcing my gpg secrets file in my system
// spark.sparkContext.hadoopConfiguration
//   .set("fs.s3a.access.key", sys.env.get("AWS_ACCESS_KEY_ID").get)
// spark.sparkContext.hadoopConfiguration
//   .set("fs.s3a.secret.key", sys.env.get("AWS_SECRET_ACCESS_KEY").get)
//
// val mydf = spark.read
//   .option("header", "true")
//   .option("inferSchema", "true")
//   .csv("s3a://datasets-rk/fakefriends.csv")
// mydf.printSchema()
// mydf.show()
