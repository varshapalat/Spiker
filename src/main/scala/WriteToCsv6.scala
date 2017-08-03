import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

class WriteToCsv6 {
  def writeToCsv6(args: Array[String]): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance
    val directory = args(0)
    println("For each partition")
    val spark = SparkSession
                .builder()
                .appName("Spark SQL data sources example")
                .master("local")
                .getOrCreate()
    import spark.implicits._
    val dataset = spark.read.option("header", true).csv(directory).as[Person]
    println("Enter output directory : ")
    val outputDir = scala.io.StdIn.readLine()

    val timeBefore3 = java.lang.System.currentTimeMillis()
    dataset.write.option("header", "true").csv(outputDir)
    val timeAfter3 = java.lang.System.currentTimeMillis()
    val timeDifference3 = timeAfter3 - timeBefore3
    println("TIME difference: " + timeDifference3)
  }
}
