/* these are generic import statements we always need */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object q4 { // all code must be inside an object
  def main(args: Array[String]) = { // this is the entry
    val conf = new SparkConf().setAppName("q4") // create a configuration
    val sc = new SparkContext(conf) // create the sc
    val input = sc.textFile("/datasets/facebook")
    //val input = sc.textFile("datasets/facebook.edge")
    val data = input.map(x => x.split("\\s+"))
    // <from, to>
    val srcFriendPair = data.map(x => (x(0).toInt, x(1).toInt))
    // <to, from>
    val reversePair = srcFriendPair.map(x => (x._2, x._1))

    // <to, <from, friendOfFriend>>
    val friendJoin = reversePair.join(srcFriendPair)
    // <from, friendOfFriend>
    val friendOfFriend = friendJoin.map(x => (x._2._1, x._2._2)).distinct().filter(x => x._1 != x._2)
    friendOfFriend.sortByKey().foreach(println)
    val counts = friendOfFriend.map(x => (x._1, 1)).reduceByKey((a, b) => a + b)
    //in your hdfs home directory
    counts.saveAsTextFile("q4fb")
  }
}
