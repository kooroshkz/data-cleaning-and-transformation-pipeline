import org.apache.spark.sql.SparkSession

object Main {

  val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipeline")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    // Default data directory
    val dataDir = "data"
    val dataFiles = FileHandler.getDatasetFiles(dataDir)

    if (dataFiles.isEmpty) {
      println("Error: No CSV or JSON files found in the 'data' directory.")
      println("Please place a CSV or JSON file in the 'data' folder or specify the file path as an argument.")
      return
    }

    if (dataFiles.length == 1) {
      // If only one file, process it
      val filePath = dataFiles(0).getAbsolutePath
      println(s"Using the only file found: $filePath")
      processFile(filePath)
    } else if (dataFiles.length > 1) {
      // Multiple files, ask user to choose
      println("Error: Multiple CSV or JSON files found in the 'data' directory. Please specify the file path.")
      dataFiles.zipWithIndex.foreach { case (file, index) =>
        println(s"${index + 1}: ${file.getName}")
      }
      println("Please specify the number corresponding to the file you want to use.")
      val fileIndex = scala.io.StdIn.readInt() - 1
      if (fileIndex >= 0 && fileIndex < dataFiles.length) {
        processFile(dataFiles(fileIndex).getAbsolutePath)
      } else {
        println("Invalid file selection.")
      }
    }
  }

  // Process the selected file
  def processFile(filePath: String): Unit = {
    // Load the dataset
    val df = loadDataset(filePath)

    // Process the data (handle missing values, type conversion, etc.)
    val cleanedDF = DataProcessor.handleMissingValues(df)
    val transformedDF = DataTransformer.transformData(cleanedDF)

    // Show the cleaned data
    transformedDF.show()

    // Save the cleaned data
    val outputFilePath = filePath.replaceAll("\\.csv$|\\.json$", "") + "_cleaned.csv"
    FileHandler.saveData(transformedDF, outputFilePath)
  }

  // Load dataset based on the extension (CSV/JSON)
  def loadDataset(filePath: String): org.apache.spark.sql.DataFrame = {
    val fileExtension = filePath.split("\\.").last.toLowerCase
    fileExtension match {
      case "csv" => spark.read.option("header", "true").csv(filePath)
      case "json" => spark.read.json(filePath)
      case _ => throw new IllegalArgumentException("Unsupported file format. Please provide a CSV or JSON file.")
    }
  }
}
