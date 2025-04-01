import java.nio.file.{Paths, Files}

object FileHandler {
  
  // Checks if the file exists
  def fileExists(filePath: String): Boolean = {
    Files.exists(Paths.get(filePath))
  }

  // Get a list of all CSV and JSON files in the data directory
  def getDatasetFiles(dataDir: String): Array[java.io.File] = {
    new java.io.File(dataDir).listFiles.filter(f => f.getName.endsWith(".csv") || f.getName.endsWith(".json"))
  }

  // Save cleaned data
  def saveData(df: org.apache.spark.sql.DataFrame, outputFilePath: String): Unit = {
    if (fileExists(outputFilePath)) {
      println(s"Warning: The file '$outputFilePath' already exists. Overwriting it.")
    }
    df.write.option("header", "true").csv(outputFilePath)
    println(s"Cleaned data saved to $outputFilePath")
  }
}
