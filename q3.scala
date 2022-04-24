/* these are generic import statements we always need */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object q3 { // all code must be inside an object
  def main(args: Array[String]) = { // this is the entry
    val conf = new SparkConf().setAppName("q3") // create a configuration
    val sc = new SparkContext(conf) // create the sc
    val input = sc.textFile("/datasets/flight")
    //val input = sc.textFile("datasets/flight.csv")
    // ITIN_ID,YEAR,QUARTER,ORIGIN,ORIGIN_STATE_NM,DEST,DEST_STATE_NM,PASSENGERS
    val data = input.filter(x => x != "ITIN_ID,YEAR,QUARTER,ORIGIN,ORIGIN_STATE_NM,DEST,DEST_STATE_NM,PASSENGERS").map(x => x.split(","))
    val origin_dst_state = data.map(x => (x(3), x(6))).distinct()
    val origin_cnt = origin_dst_state.map(x => (x._1, 1)).reduceByKey((a, b) => a + b)
    //in your hdfs home directory
    origin_cnt.saveAsTextFile("q3distinct")
  }
}
