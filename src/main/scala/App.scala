import org.apache.spark.sql.{Row}

object App extends Context {

  override val appName: String = "Spark_3_3_4"

  def main(args: Array[String]) = {


    val carDf = spark.read
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/hrdataset.csv")

    import spark.implicits._
    val hrDataDs = carDf.as[HrData]


    val findValues = List("BI", "it")

    hrDataDs
      .select("PositionID","Position")
      .filter(value => isMatchPosition(value, findValues))
      .distinct()
      .show()

    def isMatchPosition(row: Row, findValues: List[String]): Boolean = {
      findValues.exists(value => row.getString(1).toLowerCase().startsWith(value.toLowerCase()))
    }

  }

}




