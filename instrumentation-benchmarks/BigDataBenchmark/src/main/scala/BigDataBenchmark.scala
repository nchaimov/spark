import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object BigDataBenchmark {
  def main(args: Array[String]) {

    if(args.length != 4) {
      println(s"Usage: ${args(0)} path doCache parallelism")
      sys.exit
    }

    val conf = (new SparkConf()
                     .setAppName("BigDataBenchmark")
                     .set("spark.driver.maxResultSize", "12g")
                     .set("spark.default.parallelism", args(3))
               )
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val path = args(1)
    val doCache : Boolean = args(2).toBoolean
    val partitions : Integer = args(4).toInt

    val rankingsDF = sqlContext.read.parquet(s"$path/rankings.parquet")
    rankingsDF.registerTempTable("rankings")

    val uservisitsDF = sqlContext.read.parquet(s"$path/uservisits.parquet")
    uservisitsDF.registerTempTable("uservisits")

    if(doCache) { sqlContext.cacheTable("rankings"); }
    if(doCache) { sqlContext.cacheTable("uservisits"); }

    val before : Long = System.currentTimeMillis
    val query1a = sqlContext.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > 1000")
    query1a.collect()
    val query1a_finished : Long = System.currentTimeMillis
    val query1b = sqlContext.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > 100")
    query1b.collect()
    val query1b_finished : Long = System.currentTimeMillis
    val query1c = sqlContext.sql("SELECT pageURL, pageRank FROM rankings WHERE pageRank > 10")
    query1c.collect()
    val query1c_finished : Long = System.currentTimeMillis

    // Query 2
    val query2a = sqlContext.sql("SELECT SUBSTR(sourceIP, 1, 8), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 8)")
    query2a.collect()
    val query2a_finished : Long = System.currentTimeMillis
    val query2b = sqlContext.sql("SELECT SUBSTR(sourceIP, 1, 10), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 10)")
    query2b.collect()
    val query2b_finished : Long = System.currentTimeMillis
    val query2c = sqlContext.sql("SELECT SUBSTR(sourceIP, 1, 12), SUM(adRevenue) FROM uservisits GROUP BY SUBSTR(sourceIP, 1, 12)")
    query2c.collect()
    val query2c_finished : Long = System.currentTimeMillis

    // Query 3
    val query3a = sqlContext.sql("""SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE UV.visitDate > "1980-01-01" AND UV.visitDate < "1980-04-01") NUV ON (R.pageURL = NUV.destURL) GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1""")
    query3a.collect()                                                      
    val query3a_finished : Long = System.currentTimeMillis
    val query3b = sqlContext.sql("""SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE UV.visitDate > "1980-01-01" AND UV.visitDate < "1983-01-01") NUV ON (R.pageURL = NUV.destURL) GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1""")
    query3b.collect()
    val query3b_finished : Long = System.currentTimeMillis
    val query3c = sqlContext.sql("""SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) as pageRank FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE UV.visitDate > "1980-01-01" AND UV.visitDate < "2010-01-01") NUV ON (R.pageURL = NUV.destURL) GROUP BY sourceIP ORDER BY totalRevenue DESC LIMIT 1""")
    query3c.collect()
    val query3c_finished : Long = System.currentTimeMillis
    val after : Long = System.currentTimeMillis


    println()
    println(s"Configuration: path: ${path}, cached tables = ${doCache}")
    println()
    println(s"Query 1a time: ${query1a_finished - before}")
    println(s"Query 1b time: ${query1b_finished - query1a_finished}")
    println(s"Query 1c time: ${query1c_finished - query1b_finished}")
    println()
    println(s"Query 2a time: ${query2a_finished - query1c_finished}")
    println(s"Query 2b time: ${query2b_finished - query2a_finished}")
    println(s"Query 2c time: ${query2c_finished - query2b_finished}")
    println()
    println(s"Query 3a time: ${query3a_finished - query2c_finished}")
    println(s"Query 3b time: ${query3b_finished - query3a_finished}")
    println(s"Query 3c time: ${query3c_finished - query3b_finished}")
    println()
    println(s"Overall time: ${after - before}")
    println("\n\n")

  }
}

