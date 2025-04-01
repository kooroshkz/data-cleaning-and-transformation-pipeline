import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object DataProcessor {

  val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipeline")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Handle missing values
  def handleMissingValues(df: DataFrame): DataFrame = {
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
}
