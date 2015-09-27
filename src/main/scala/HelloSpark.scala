import org.apache.spark._
/** c
 * Created by gpzpati on 9/23/15.
 */
object HelloSpark {


  def main(argv:Array[String]): Unit ={
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("HelloSpark")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("/Users/gpzpati/software/spark-1.5.0/README.md")

    val words = lines.flatMap(_.split(" ")).map(word => (word, 1))
    val wordCounts = words.reduceByKey((a,b) => a+b)
    //wordCounts.foreach(println)

    wordCounts.takeOrdered(10).foreach(println)
    wordCounts.countBy

  }


}
