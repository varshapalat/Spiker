import org.apache.spark.sql.SparkSession

class LoadFromDataset1 {

  def loadFromDataset1(args: Array[String]): Unit = {
    val directory = args(0)
    val connectionString = args(1)
    println("Load from Dataset")
    val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
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
  }
}
