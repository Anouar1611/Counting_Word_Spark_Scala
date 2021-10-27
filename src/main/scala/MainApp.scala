
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/* 
* 
* This is a simple application for counting the number of each word in a given file
* 
* */
object MainApp {

  def main(args: Array[String]): Unit = {
    
    val sparkConf = new SparkConf().setAppName("Counting words simple application")
    val sc = new SparkContext(sparkConf)
    val sourceFile = args(0)
    val targetFile = args(1)

    val textFile = sc.textFile(sourceFile)

    val tokenizedTextFile = textFile.flatMap(line => line.split(" "))
    val countPrep = tokenizedTextFile.map(word => (word, 1))

    val counts = countPrep.reduceByKey((accValue, newValue) =>  accValue + newValue)
    val sortedCounts = counts.sortBy(pair => pair._2, false)
    sortedCounts.saveAsTextFile(targetFile)
  }
}