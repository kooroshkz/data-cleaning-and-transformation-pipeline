import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object DataCleaning {

  // Initialize Spark session
  val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipeline")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Load a dataset (example: Titanic dataset)
  def loadDataset(filePath: String, format: String = "csv"): DataFrame = {
    spark.read.format(format).option("header", "true").load(filePath)
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
    val filePath = "data/titanic.csv"  // Change the path to your dataset
    val df = loadDataset(filePath)

    val cleanedDF = handleMissingValues(df)
    val transformedDF = convertDataTypes(cleanedDF)

    // Show cleaned data
    transformedDF.show()
    
    // Save cleaned data
    transformedDF.write.option("header", "true").csv("data/titanic_cleaned.csv")
  }
}
