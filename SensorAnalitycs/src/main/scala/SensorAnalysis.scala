import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

case class SensorStats(tempMin: String, tempMax: String, tempSum: String, tempCnt: Long, presence: Boolean, presenceCnt: Long)

object SensorAnalysis {

  val DATA_FOLDER = "/home/mike/gen_data"
  val OUTPUT_FOLDER = "/home/mike/output_data"
  type Key = (String, String)
  type Value = (String, String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext

    if(args.length == 0) {
      processTimeslot(sc, false, "ods1")
      processTimeslot(sc, true, "ods2")
    }
    else if(args.length > 0 && args(0) == "true"){
      recalculateMinutesToHours(sc, OUTPUT_FOLDER + "/ods1", OUTPUT_FOLDER + "/ods3")
    }
    else if(args.length == 3){
      try {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val start = dateFormat.parse(args(1))
        val end = dateFormat.parse(args(2))

        if(start.compareTo(end) <= 0){
          processTimeslot(sc, false, "ods1", args(1), args(2))
          processTimeslot(sc, true, "ods2", args(1), args(2))
        } else System.out.println("Error: Start time is greater then end time")
      } catch {
        case java.text.ParseException =>
          System.out.println("Specified dates could not be parsed")
      }
    }
    else System.out.println("Wrong parameters list")

    spark.close()
  }

  def processTimeslot(sc: SparkContext, ifHour: Boolean, outputDir: String, startDate: String = "", endDate: String = ""): Unit = {
    val ds1: RDD[(Key, Value)] = sc.textFile(DATA_FOLDER + "/ds1.csv")
      .map(_.split(","))
      .map( row => ((row(0), row(1)), (row(2), row(3))))

    val ds2: RDD[(Key, Value)] = if(startDate == "" && endDate == ""){
      sc.textFile(DATA_FOLDER + "/ds2.csv")
        .map(_.split(","))
        .map( row => ((row(0), row(1)), (row(2), row(3))))
    } else {
      sc.textFile(DATA_FOLDER + "/ds2.csv")
        .map(_.split(","))
        .map( row => ((row(0), row(1)), (row(2), row(3)))).filter(row => dateFilterer(row._2._1.trim, startDate, endDate))
    }

    val dataFormatted = ds1.join(ds2).map{ case ((_, _), ((a, b), (c, d))) => (c, b, a, d) }

    val dataTimeSlot: RDD[(Key, Value)] = dataFormatted.map(i => (correctDate(i._1, ifHour), i._2, i._3, i._4))
      .map{ case(a, b, c, d) => ((a, b), (c, d))}

    val zero = SensorStats("", "", "", 0, false, 0)
    val statsPerTimeSlot = dataTimeSlot.aggregateByKey(zero)(calcSensorStats, combinePartStats)

    statsPerTimeSlot.map{ case((ts, id), sd) => formatOutput((ts, id), sd)}.saveAsTextFile(OUTPUT_FOLDER + "/" + outputDir)
  }

  def dateFilterer(date: String, startDate: String, endDate: String): Boolean = {
    val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")

    val dateTime = LocalDateTime.parse(date, dateTimeFormatter)
    val start = LocalDateTime.ofInstant(dateFormatter.parse(startDate).toInstant(), ZoneId.systemDefault)
    val end = LocalDateTime.ofInstant(dateFormatter.parse(endDate).toInstant(), ZoneId.systemDefault).plusDays(1)

    if(dateTime.compareTo(start) >= 0 && dateTime.compareTo(end) <= 0)
      true
    else
      false
  }

  def recalculateMinutesToHours(sc: SparkContext, inputDir: String, outputDir: String): Unit = {
    val data: RDD[(Key, SensorStats)] = sc.textFile(inputDir)
      .map(_.split(","))
      .map(row => ((correctDate(row(0), true), row(1)),
        SensorStats(row(2), row(4), if(row(5) != "") (row(5).toDouble * row(6).toLong).toString else row(5),
          row(6).toLong, row(7).toBoolean, row(8).toLong)))

    val perHourStats = data.reduceByKey((initial, current) => combinePartStats(initial, current))

    perHourStats.map{ case((ts, id), sd) => formatOutput((ts, id), sd)}.saveAsTextFile(outputDir)
  }

  def correctDate(dateString: String, ifHour: Boolean): String = {
    if(!ifHour){
      val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = formatter.parse(dateString)
      val correctedMinutes = (date.getMinutes / 15).toInt * 15

      if (correctedMinutes != 0)
        dateString.substring(0, dateString.indexOf(":")) + ":" + correctedMinutes + ":00"
      else
        dateString.substring(0, dateString.indexOf(":")) + ":00:00"
    }
    else {
      dateString.substring(0, dateString.indexOf(":")) + ":00:00"
    }
  }
  
  def calcSensorStats(initial: SensorStats, sensorData: Value): SensorStats = sensorData._1 match {
    case "temperature" =>
      val currentVal = sensorData._2
      val tempCount = initial.tempCnt

      if(initial.tempCnt == 0){
        SensorStats(currentVal, currentVal, currentVal, tempCount + 1, initial.presence, initial.presenceCnt)
      }
      else {
        val minTemp = if(initial.tempMin.toDouble <= currentVal.toDouble) initial.tempMin else currentVal
        val maxTemp = if(initial.tempMax.toDouble >= currentVal.toDouble) initial.tempMin else currentVal
        val sumTemp = (initial.tempSum.toDouble + currentVal.toDouble).toString
        
        SensorStats(minTemp, maxTemp, sumTemp, tempCount + 1, initial.presence, initial.presenceCnt)
      }

    case "presence" =>
      val presenceCount = initial.presenceCnt + 1
      val setPresence = true
      SensorStats(initial.tempMin, initial.tempMax, initial.tempSum, initial.tempCnt, setPresence, presenceCount)

    case _ => initial
  }
  
  def combinePartStats(initial: SensorStats, current: SensorStats): SensorStats = {
    var minTemp = initial.tempMin
    var maxTemp = initial.tempMax
    var sumTemp = initial.tempSum
    var tempCounter = initial.tempCnt
    var presenceStatus = initial.presence
    
    if(initial.tempCnt == 0) {
      minTemp = current.tempMin
      maxTemp = current.tempMax
      sumTemp = current.tempMax
      tempCounter = current.tempCnt
    } 
    else {
      minTemp = if(minTemp.toDouble <= current.tempMin.toDouble) minTemp else current.tempMin
      maxTemp = if(maxTemp.toDouble >= current.tempMax.toDouble) maxTemp else current.tempMax
      sumTemp = (sumTemp.toDouble + current.tempSum.toDouble).toString
      tempCounter = tempCounter + current.tempCnt
    }

    if(!presenceStatus) presenceStatus = current.presence

    SensorStats(minTemp, maxTemp, sumTemp, tempCounter, presenceStatus, initial.presenceCnt + current.presenceCnt)
  }

  def formatOutput(keys: (String, String), stats: SensorStats): String = {
    val avgTemp = if(stats.tempSum != "") stats.tempSum.toDouble / stats.tempCnt else stats.tempSum
    keys._1.trim + "," + keys._2.trim + "," + stats.tempMin.trim + "," + stats.tempMax.trim +
      "," + avgTemp + "," + stats.tempCnt + "," + stats.presence + "," + stats.presenceCnt
  }
}
