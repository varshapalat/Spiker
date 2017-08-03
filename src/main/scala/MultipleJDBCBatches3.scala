import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

class MultipleJDBCBatches3 {
  def multipleJDBCBatches3(args: Array[String]): Unit = {
    println("For each partition")
    val directory = args(0)
    val connectionString = args(1)

    val url = connectionString
    val username = "sa"
    val password = "C0mplexPwd"
    val spark = SparkSession
                .builder()
                .appName("Spark SQL data sources example")
                .master("local")
                .getOrCreate()
    import spark.implicits._
    val dataset = spark.read.option("header", true).csv(directory).as[Person]

    val conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(false)
    val stmt = conn.prepareStatement("insert into dbo.Users  (first_name, last_name, blr) values (?, ?, ?)")
    dataset.collect().foreach { ds =>
        stmt.setString(1, ds.first_name)
        stmt.setString(2, ds.last_name)
        stmt.setString(3, ds.blr)
        stmt.addBatch()
    }

    println("Third Method: Insert statements in a multiple batches")
    val timeBefore2 = java.lang.System.currentTimeMillis()
    stmt.executeBatch()
    conn.commit()
    val timeAfter2 = java.lang.System.currentTimeMillis()
    val timeDifference2 = timeAfter2 - timeBefore2
    println("TIME difference: " + timeDifference2)
  }
}
