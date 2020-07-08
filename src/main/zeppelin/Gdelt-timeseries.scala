import com.aamend.texata.gdelt._
val inputGkg = "/root/GIT/lamastex/spark-texata-2020/src/test/resources/com/aamend/texata/gdelt/gkg.csv"
val inputEvent = "/root/GIT/lamastex/spark-texata-2020/src/test/resources/com/aamend/texata/gdelt/events.csv"

val eventDS = spark.read.gdeltEVENT(inputEvent)
val gkgDS = spark.read.gdeltGKG(inputGkg)

%sql
SELECT * FROM oil_gas
WHERE year(date) > 2014
ORDER BY articles DESC
LIMIT 1000
val oilGKGDF = gkgDS.filter(c => c.themes.contains("ENV_GAS") || c.themes.contains("ENV_OIL"))
                    .flatMap(c => c.eventIds.map(_ -> "DUMMY"))
                    .toDF("eventId", "_dummy")

val oilEventDF = eventDS.toDF()
                        .join(oilGKGDF, "eventId")

val mve = mostImportantEvents
val df = spark.read.parquet("oil_events")
        .withColumnRenamed("date", "_date")
val mveEvents = mve
                .join(df, df("_date") === mve("date") && df("actionGeo.countryCode").as("_country") === mve("country")).cache()

mveEvents.select("date", "country", "sourceUrl").show

import com.aamend.texata.html.HtmlFetcher
val urlRDD = mveEvents.select("sourceUrl").distinct().rdd.map(_.getAs[String]("sourceUrl"))
import com.gravity.goose.{Configuration, Goose}
import org.apache.spark.rdd.RDD
def fetcher(iterator: Iterator[String]) = {
    val conf: Configuration = new Configuration
    conf.setBrowserUserAgent("mozilla texata")
    conf.setEnableImageFetching(false)
    conf.setConnectionTimeout(12000)
    conf.setSocketTimeout(12000)
    val goose = new Goose(conf)
    for (url <- iterator) yield {
        try {
                val article = goose.extractContent(url)
                   (
                        url,
                        article.title,
                        article.metaDescription,
                        article.cleanedArticleText,
                        article.tags.toSet.toArray
                    )
            } catch {
                        case _: Throwable => ( 
                                                url,
                                                null,
                                                null,
                                                null,        
                                                Array.empty[String]
                                                )
                    }
    }
}

val articleDF = urlRDD.distinct.mapPartitions(fetcher).toDF("_URL", "title", "description", "text", "tags")
val mveEnrichedDF = mveEvents.join(articleDF, col("_URL") === col("sourceURL")).orderBy(col("date"))
                             .select("date", "country", "goldsteinScale", "sourceUrl", "title", "description", "tags")

mveEnrichedDF.cache()

mveEnrichedDF.filter(col("title")
             .isNotNull && length(col("title")) > 0)
             .rdd.map(r => {
                                (r.getAs[String]("country"), 
                                (r.getAs[Float]("goldsteinScale"), 
                                r.getAs[String]("title"), r.getAs[String]("description"), 
                                r.getAs[Date]("date")))
                            }).groupByKey().mapValues(_.toList.maxBy(v => math.abs(v._1)))
                            .map({ case (country, (goldstein, title, description, date)) =>
                                        (country, goldstein, title, description, date)
                                })
                            .toDF("country", "goldstein", "title", "description", "date")
                            .drop("description")
                            .withColumn("abs", abs(col("goldstein")))
                            .orderBy(desc("abs")).show(truncate = false)
