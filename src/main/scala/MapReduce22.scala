import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

class MapReduce22 {

  def mapReduce22(args: Array[String]): Unit = {
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
    val statement = conn.createStatement()
    val begining = "INSERT INTO db.Users VALUES "
    val insertStatements = dataset
              .map(d => String.format("('%s', '%s' , '%s')", d.first_name, d.last_name, d.blr))
              .reduce((s1, s2) => s1 + "," + s2)
      val fullStatement = begining + insertStatements
            statement.addBatch(fullStatement)
        println("Second method: Insert statements in a single batch")
        val timeBefore1 = java.lang.System.currentTimeMillis()
        //    statement.executeUpdate(insertStatements)
            statement.executeBatch()
        val timeAfter1 = java.lang.System.currentTimeMillis()
        val timeDifference1 = timeAfter1 - timeBefore1
        println("TIME difference: " + timeDifference1)
  }
}
