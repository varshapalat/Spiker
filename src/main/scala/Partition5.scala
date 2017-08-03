import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

class Partition5 {
  def partition5(args: Array[String]): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance
    val directory = args(0)
    val connectionString = args(1)
    println("For each partition")
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

    println("Fifth Method: For each partition")
    println("Enter number of partitions : ")
    val input = scala.io.StdIn.readInt()
    dataset.repartition(input)
    dataset.foreachPartition(iter => {
          val connection = DriverManager.getConnection(url, username, password)
          val statement1 = connection.prepareStatement("insert into dbo.Users  (first_name, last_name, blr) values (?, ?, ?)")
          iter.foreach(x => {
                statement1.setString(1, x.first_name)
                statement1.setString(2, x.last_name)
                statement1.setString(3, x.blr)
                statement1.addBatch()
      })
    val timeBefore3 = java.lang.System.currentTimeMillis()
    statement1.executeBatch()
    connection.commit()
    val timeAfter3 = java.lang.System.currentTimeMillis()
    val timeDifference3 = timeAfter3 - timeBefore3
    println("TIME difference: " + timeDifference3)
    });
  }
}
