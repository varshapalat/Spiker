import java.sql.DriverManager
import org.apache.spark.sql.SparkSession

case class Person (var first_name: String, last_name: String, blr: String)

class Spiker {

  def ExecuteInsert(): Unit = {
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
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

    val prop = new java.util.Properties
    prop.setProperty("driver", driver)
    prop.setProperty("user", username)
    prop.setProperty("password", password)

    println("First method : directly from dataset")
    val timeBefore = java.lang.System.currentTimeMillis()
    dataset.write.mode("append").jdbc(url, "dbo.Users", prop)
    val timeAfter = java.lang.System.currentTimeMillis()
    val timeDifference = timeAfter - timeBefore

    println("TIME difference:" + timeDifference)

    val conn = DriverManager.getConnection(url, username, password)
    val statement = conn.createStatement()
//        val insertStatements = dataset
//          .map(d => String.format("INSERT INTO dbo.Users VALUES ('%s', '%s' , '%s')", d.first_name, d.last_name, d.blr))
//          .reduce((s1, s2) => s1 + ";" + s2)
//        statement.addBatch(insertStatements)
//    println("Second method: Insert statements in a single batch")
//    val timeBefore1 = java.lang.System.currentTimeMillis()
//    //    statement.executeUpdate(insertStatements)
//        statement.executeBatch()
//    val timeAfter1 = java.lang.System.currentTimeMillis()
//    val timeDifference1 = timeAfter1 - timeBefore1
//    println("TIME difference: " + timeDifference1)
//
    conn.setAutoCommit(false)
    val stmt = conn.prepareStatement("insert into dbo.Users  (first_name, last_name, blr) values (?, ?, ?)")
    dataset.collect().foreach{ ds =>
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
//
//    val arr = dataset.collect.map{row => new Person(row.first_name, row.last_name, row.blr)}
//    for (person <-arr){
//      val query = "insert into dbo.Users values('" + person.first_name + "','" + person.last_name+  "','" + person.blr +"')"
//      statement.addBatch(query)
//    }
//    println("Forth Method: Insert statements in a multiple batches after converting to array")
//    val timeBefore3 = java.lang.System.currentTimeMillis()
//     statement.executeBatch()
//    conn.commit()
//
//    val timeAfter3 = java.lang.System.currentTimeMillis()
//    val timeDifference3 = timeAfter3 - timeBefore3
//    println("TIME difference: " + timeDifference3)
//    println("NUMBER"+spark.sparkContext.getExecutorMemoryStatus)

    val sc = spark.sparkContext
    println("Fifth Method: For each partition")
    println("EXecutors" + sc.getExecutorStorageStatus)
    println(sc.getExecutorMemoryStatus)
    println(sc.requestExecutors(4))

    dataset.repartition(10000)
    dataset.foreachPartition(iter =>{

      val connection = DriverManager.getConnection(url, username, password)
      val statement1 = connection.prepareStatement("insert into dbo.Users  (first_name, last_name, blr) values (?, ?, ?)")
      iter.foreach(x =>{
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
    })
  }
}