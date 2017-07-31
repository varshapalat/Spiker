import java.sql.{Connection, DriverManager, Statement}
import org.apache.spark.sql.SparkSession


case class Person (first_name: String, last_name: String, blr: String)

class Spiker {
  def Execute(): Unit = {
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    val url = "jdbc:sqlserver://127.0.0.1:1433;DatabaseName=RETAIL_CLIENT;"
    val username = "sa"
    val password = "C0mplexPwd"
    val spark = SparkSession
      .builder()
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .master("local")
      .getOrCreate()


    import spark.implicits._
    val dataset = spark.read.option("header", true).csv("D:\\McKinsey\\untitled\\input.csv").as[Person]
    val prop = new java.util.Properties
    prop.setProperty("driver", driver)
    prop.setProperty("user", username)
    prop.setProperty("password", password)

    val timeBefore = java.lang.System.currentTimeMillis()
    dataset.write.mode("append").jdbc(url, "dbo.Users", prop)
    val timeAfter = java.lang.System.currentTimeMillis()
    val timeDifference = timeAfter - timeBefore
    println("TIME difference:" + timeDifference)
    println("DONE")

    var statement: Statement = null
    val conn = DriverManager.getConnection(url, username, password)
    statement = conn.createStatement()
    val time2 = java.lang.System.currentTimeMillis()


    val insertStatements = dataset
      .map(d => String.format("INSERT INTO dbo.Users VALUES ('%s', '%s' , '%s')", d.first_name, d.last_name, d.blr))
      .reduce((s1, s2) => s1 + ";" + s2)
    statement.addBatch(insertStatements)

//    import spark.implicits._
//    val insertStmts = dataset
//      .map(d => "")
//    insertStmts.foreach((insertStmt1) => statement.addBatch(insertStmt1))

    val timeBefore1 = java.lang.System.currentTimeMillis()
    //    statement.executeUpdate(insertStatements)
    statement.executeBatch()
    val timeAfter1 = java.lang.System.currentTimeMillis()
    val timeDifference1 = timeAfter1 - timeBefore1
    println("TIME difference: " + timeDifference1)

    //
  }
}