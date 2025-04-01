import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Paths, Files}
import java.io.File

object DataCleaning {

  // Initialize Spark session
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

  // Handle missing values
  def handleMissingValues(df: DataFrame): DataFrame = {
    // Fill missing values in numeric columns with the mean
    val numericCols = df.columns.filter(col => df.schema(col).dataType.typeName == "double" || df.schema(col).dataType.typeName == "float")
    var cleanedDF = df
    numericCols.foreach { col =>
      val meanValue = df.agg(avg(col)).first().getDouble(0)
      cleanedDF = cleanedDF.na.fill(Map(col -> meanValue))
    }
    cleanedDF
  }

  // Data type conversion (e.g., convert a column to a numeric type)
  def convertDataTypes(df: DataFrame): DataFrame = {
    df.withColumn("Age", $"Age".cast("double"))
      .withColumn("Fare", $"Fare".cast("double"))
  }

  // Main method to run the pipeline
  def main(args: Array[String]): Unit = {
    // Default path to the data directory
    val dataDir = "data"

    // Get a list of all CSV and JSON files in the data directory
    val dataFiles = new File(dataDir).listFiles.filter(f => f.getName.endsWith(".csv") || f.getName.endsWith(".json"))

    if (dataFiles.isEmpty) {
      println("Error: No CSV or JSON files found in the 'data' directory.")
      println("Please place a CSV or JSON file in the 'data' folder or specify the file path as an argument.")
      return
    }

    if (dataFiles.length == 1) {
      // Use the only file found (either CSV or JSON)
      val filePath = dataFiles(0).getAbsolutePath
      println(s"Using the only file found: $filePath")
      processFile(filePath)
    } else if (dataFiles.length > 1) {
      // If multiple files are found, ask the user to specify one
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
    } else {
      // If no arguments are passed, check if the default data folder has one CSV or JSON file
      if (args.isEmpty) {
        println("No input file specified. Searching for a CSV or JSON file in the 'data' folder...")
        if (dataFiles.length == 1) {
          val filePath = dataFiles(0).getAbsolutePath
          println(s"Using default file: $filePath")
          processFile(filePath)
        }
      }
    }
  }

  // Method to process the file
  def processFile(filePath: String): Unit = {
    // Ensure the file exists
    if (!Files.exists(Paths.get(filePath))) {
      println(s"Error: The file at $filePath does not exist!")
      return
    }

    // Load dataset (either CSV or JSON based on the file extension)
    val df = loadDataset(filePath)

    // Process the dataset
    val cleanedDF = handleMissingValues(df)
    val transformedDF = convertDataTypes(cleanedDF)

    // Show cleaned data
    transformedDF.show()

    // Generate output file path based on the input file name
    val outputFilePath = filePath.replaceAll("\\.csv$|\\.json$", "") + "_cleaned.csv"
    
    // Save cleaned data (output will always be in CSV format)
    transformedDF.write.option("header", "true").csv(outputFilePath)
    println(s"Cleaned data saved to $outputFilePath")
  }
}
