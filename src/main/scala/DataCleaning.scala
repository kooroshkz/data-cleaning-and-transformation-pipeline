import org.apache.spark.sql.{SparkSession, DataFrame}

object DataCleaning {

  val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipeline")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Load a dataset (CSV or JSON based on file extension)
  def loadDataset(filePath: String): DataFrame = {
    val fileExtension = filePath.split("\\.").last.toLowerCase

    fileExtension match {
      case "csv" => 
        spark.read.format("csv").option("header", "true").load(filePath)
      case "json" => 
        spark.read.format("json").load(filePath)
      case _ => 
        throw new IllegalArgumentException("Unsupported file format. Please provide a CSV or JSON file.")
    }
  }
}
