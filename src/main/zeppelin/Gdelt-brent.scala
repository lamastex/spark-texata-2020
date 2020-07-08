// Requires TrendCalculus library

import com.aamend.texata.trend.DateUtils.Frequency
import com.aamend.texata.trend.SeriesUtils.FillingStrategy
import com.aamend.texata.trend._

val dateUDF = udf( (s: String) =>
    new java.sql.Date(
        new java.text.SimpleDateFormat("yyyy-MM-dd")
        .parse(s)
        .getTime
    )
)
        
val valueUDF = udf( (s: String) => s.toDouble)
    
val DF = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/brent.csv")
    .filter(year(col("DATE")) >= 2015)
                    
DF.show
DF.createOrReplaceTempView("brent")

val trendDF = DF.rdd.map(r => ("DUMMY", Point(r.getAs[java.sql.Date]("DATE").getTime,r.getAs[Double]("VALUE"))))
    .groupByKey()
    .mapValues(it => {
        val series = it.toArray.sortBy(_.x)
        SeriesUtils.completeSeries(series, Frequency.DAY, FillingStrategy.LOCF)
    })
    .flatMap({ case ((s), series) =>
        new TrendCalculus(series, Frequency.MONTH)
            .getTrends
            .filter { trend =>
                trend.reversal.isDefined
                }
        .map { trend =>
            (trend.trend.toString, trend.reversal.get.x, trend.reversal.get.y)
            }
        .map { case (trend, x, y) =>
            (trend, new java.sql.Timestamp(x), y)
            }
        })
    .toDF("trend", "x", "y")
        
trendDF.show

val trendUDF = udf((t: String) => if(t == null) "NEUTRAL" else t)
trendDF.join(DF, trendDF("x") === DF("DATE"), "right_outer")
    .withColumn("TREND", trendUDF(col("trend")))
    .select("DATE", "VALUE", "TREND")
    .createOrReplaceTempView("trends")