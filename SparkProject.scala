import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkProject {

    def main(args:Array[String]):Unit= {

        val spark = SparkSession.builder
          .master("local[4]")
          .appName("Moja-applikacja")
          .getOrCreate()

        val filePath="src/main/scala/actors.txt"

        //Chaining multiple options
        val df2 = spark.read.options(Map("inferSchema"->"true","sep"->",","header"->"true")).csv(filePath)
        df2.show(false)
        df2.printSchema()
        val Df = df2.withColumn("current epoch time",unix_timestamp().as("current epoch time"))
        Df.show(false)
        val sortedDf = Df.sort(col("imdb_name_id").asc)
        sortedDf.show(false)
    }
}