
import org.scalatest.FlatSpec

class SpikerTest extends FlatSpec{
  "A Stack" should "pop values in last-in-first-out order" in {
    println("Zero")
  }
  it should "ExecuteInsert" in {
    new Spiker().ExecuteInsert()
  }
   it should "For each partition" in {
     new Partition5().main(new Array[String](0))
   }
  it should "load data from dataset" in {
    new LoadFromDataset1().main(new Array[String](0))
  }
  it should "multiple batches" in {
    new MultipleJDBCBatches3().main(new Array[String](0))
  }
  it should "map reduce" in {
    new MapReduce2().main(new Array[String](0))
  }
  it should "array batch" in {
    new ArrayBatch4().main(new Array[String](0))
  }

}
