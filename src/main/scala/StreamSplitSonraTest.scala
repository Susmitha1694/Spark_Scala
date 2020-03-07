import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs._

object StreamSplitSonraTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("StreamSplittingScala").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

        val inputPath = "D:\\UCD_Masters\\job_applications\\sonra\\mobile_files_check"
        val usagePath = "D:\\UCD_Masters\\job_applications\\sonra\\output\\usage"
        val topupPath = "D:\\UCD_Masters\\job_applications\\sonra\\output\\topup"
        val usageCheckpointPath = "D:\\UCD_Masters\\job_applications\\sonra\\output\\usageLogs"
        val topupCheckPointPath = "D:\\UCD_Masters\\job_applications\\sonra\\output\\topupLogs"

//    val inputPath = args(0)
//    val usagePath = args(1)+"\\usage"
//    val topupPath = args(1)+"\\topup"
//    val usageCheckpointPath = args(1)+"\\usageLogs"
//    val topupCheckPointPath = args(1)+"\\topupLogs"

    println("input path : "+inputPath)
    println("usagePath : "+usagePath)
    println("topupPath : "+topupPath)

    //case class MobileSchema(splitCriteria: String, num: String, onDate: String, onTime: String, type_str: String, time_minutes: String)

    val mobileSchema = new StructType().add("splitCriteria", "string")
      .add("num", "string")
      .add("onDate", "string")
      .add("onTime", "string")
      .add("type_str", "string")
      .add("time_minutes", "integer")

    val mobileDataFrame = spark
      .readStream
      .format("csv")
      .option("delimiter", " ")
      .schema(mobileSchema)
      .csv(inputPath)

    //mobileDataFrame.printSchema()

    mobileDataFrame.createOrReplaceTempView("tempDF")
    var usageDataFrame = spark.sql("select * from tempDF where splitCriteria = 'USAGE'")
    var topupDataFrame = spark.sql("select * from tempDF where splitCriteria = 'TOPUP'")

    usageDataFrame.writeStream
      .format("console")
      .format("csv")
      .option("delimiter"," ")
      .option("path",usagePath)
      .option("checkpointLocation",usageCheckpointPath)
      .outputMode("append")
      .start()

    topupDataFrame.writeStream
      .format("console")
      .format("csv")
      .option("delimiter"," ")
      .option("path",topupPath)
      .option("checkpointLocation",topupCheckPointPath)
      .outputMode("append")
      .start()


    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val status =  fs.listStatus(new Path(usagePath))
    for(file <- status){
      var fileName = file.getPath.getName()
      if((fileName contains(".csv")) && !(fileName contains("usage"))) {
        fs.rename(new Path(file.getPath().toString) , new Path(usagePath+"\\usage"+fileName.substring(5,10)+".tsv"))
        fs.delete(new Path(file.getPath().toString) , true)
      }
    }

    spark.streams.awaitAnyTermination()
 }
}
