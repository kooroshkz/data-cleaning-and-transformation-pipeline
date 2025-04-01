import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object DataTransformer {

  val spark: SparkSession = SparkSession.builder()
    .appName("DataCleaningPipeline")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Example transformation: cast columns to appropriate types
  def transformData(df: DataFrame): DataFrame = {
    df.withColumn("Age", $"Age".cast("double"))
      .withColumn("Fare", $"Fare".cast("double"))
  }

  // Additional transformations can be added here as needed
}
