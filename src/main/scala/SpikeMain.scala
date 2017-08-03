

case class Person (var first_name: String, last_name: String, blr: String)

object SpikeMain {

  def main(args: Array[String]): Unit = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance
    val directory = args(0)
    println("Directory is: " + directory)
    val methodNumber = args(1)
    println("Method is: " + methodNumber)
    val connectionString = args(2)
    println("Connection String is: " + connectionString)
    val arr = new Array[String](2)
    arr(0) = directory
    arr(1) = connectionString
    if(methodNumber == "1") {
      new LoadFromDataset1().loadFromDataset1(arr)
    }

    if (methodNumber == "5") {
      new Partition5().partition5(arr)
    }

    if(methodNumber == "3") {
      new MultipleJDBCBatches3().multipleJDBCBatches3(arr)
    }
    if(methodNumber == "2") {
      new MapReduce2().mapReduce2(arr)
    }
    if(methodNumber == "4") {
      new ArrayBatch4().arrayBatch4(arr)
    }
    if(methodNumber == "22") {
      new MapReduce22().mapReduce22(arr)
    }
    if(methodNumber == "6") {
      new WriteToCsv6().writeToCsv6(arr)
    }

  }
}
