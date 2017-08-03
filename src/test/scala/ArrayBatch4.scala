import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

class ArrayBatch4 {

  def main(args: Array[String]): Unit = {
    val url = "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=RETAIL_CLIENT;EnableBulkLoad=true;BulkLoadBatchSize=10"
    val username = "sa"
    val password = "C0mplexPwd"
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .config("spark.driver.memory", "3")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val dataset = spark.read.option("header", true).csv("D:\\McKinsey\\untitled\\input.csv").as[Person]

    val conn = DriverManager.getConnection(url, username, password)
    val statement = conn.createStatement()
    val arr = dataset.collect.map{row => new Person(row.first_name, row.last_name, row.blr)}
        for (person <-arr){
          val query = "insert into dbo.Users values('" + person.first_name + "','" + person.last_name+  "','" + person.blr +"')"
          statement.addBatch(query)
        }
        println("Forth Method: Insert statements in a multiple batches after converting to array")
        val timeBefore3 = java.lang.System.currentTimeMillis()
         statement.executeBatch()
        conn.commit()

        val timeAfter3 = java.lang.System.currentTimeMillis()
        val timeDifference3 = timeAfter3 - timeBefore3
        println("TIME difference: " + timeDifference3)
        println("NUMBER"+spark.sparkContext.getExecutorMemoryStatus)
  }
}
