import com.typesafe.scalalogging._
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark._
import scala.collection.mutable.Map
/** c
 * Created by gpzpati on 9/23/15.
 */
object HelloSpark {


  def main(argv:Array[String]): Unit ={
    println("Entering HelloSpark.main")
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("HelloSpark")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("/Users/gpzpati/workspace/RStudio/R_validationresults.csv")

    val records = lines.filter(s => !s.equals("sep=~")).map(s => mapValues(s))
    records.take(5).foreach(println)

   // records.foreach(println)

    println("Exiting HelloSpark.main")
  }

  def mapValues(lineStr:String):Map[String,String]={
    println ("Value of lineStr " + lineStr)
    val line = lineStr.split("~")
    println( "Value of line " + line.mkString(","))

    val featureRequestMap = Map[String,String]()
    featureRequestMap += ("attorneyPresent" -> line(0))
    featureRequestMap += ("binAccidentCode1" -> line(1))
    featureRequestMap += ("binNumDaybwtDOLandReported" -> line(2))
    featureRequestMap += ("binPolicyPersistency" -> line(3))
    featureRequestMap += ("binPolicyState" -> line(4))
    featureRequestMap += ("binnumflagexposure" -> line(8))
    featureRequestMap += ("providerPres" -> line(9))
    featureRequestMap += ("claimnumber" -> line(11))
    featureRequestMap += ("exposureNumber" -> line(12))

    featureRequestMap += ("f7dol30dayfromcovadded" -> line(5))
    featureRequestMap += ("f14multivanclaimpolicy" -> line(6))
    featureRequestMap += ("f19priorsiuinvolvedclaimant" -> line(7))
    featureRequestMap += ("vehicletotallossindicator" -> line(10))
    featureRequestMap += ("predictedProbability" -> line(18))
    featureRequestMap
  }
}
