package com.whylogs.spark

import com.whylogs.core.DatasetProfile
import com.whylogs.core.message.InferredType
import org.apache.spark.sql.functions
import org.apache.spark.whylogs.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters.collectionAsScalaIterableConverter

case class Metric(featureName: String, metricName: String, value: Double)

/**
 * Example of extracting metrics out of whylogs
 */
class ExtractMetricsTest extends AnyFunSuite with SharedSparkContext {
  test("demo") {
    val _spark = spark
    import _spark.implicits._

    // assuming the parquet was written from this example:
    // https://github.com/whylabs/whylogs-examples/blob/mainline/scala/src/main/scala/WhyLogsDemo.scala
    val df = spark.read.parquet("profiles_parquet")
    df.printSchema()

    // extracting a list of metrics - mostly for numerical values
    val extractFunc = functions.udf((bytes: Array[Byte]) => {
      val msg = DatasetProfile.parse(new ByteArrayInputStream(bytes))
      msg.getColumns.values().asScala.flatMap(columnProfile => {
        Seq(
          Metric(columnProfile.getColumnName, "variance", columnProfile.getNumberTracker.getVariance.variance()),
          Metric(columnProfile.getColumnName, "unique_values", columnProfile.getCardinalityTracker.getEstimate),
          Metric(columnProfile.getColumnName, "max", columnProfile.getNumberTracker.getHistogram.getMaxValue),
          Metric(columnProfile.getColumnName, "min", columnProfile.getNumberTracker.getHistogram.getMinValue),
          Metric(columnProfile.getColumnName, "median", columnProfile.getNumberTracker.getHistogram.getQuantile(0.5)),
          Metric(columnProfile.getColumnName, "count", columnProfile.getCounters.getCount),
          Metric(columnProfile.getColumnName, "null_count", columnProfile.getSchemaTracker.getTypeCounts.get(InferredType.Type.NULL) * 1.0)
        )
      }).toArray
    })
    val whyProfile = df("why_profile")
    val metricDf = df.withColumn("columns", functions.explode(extractFunc(whyProfile))).drop(whyProfile)

    // flatten out the metrics
    metricDf.toDF().select($"columns.*").show(1000, truncate = true)
  }
}
