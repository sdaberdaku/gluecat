package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.expressions.Expressions
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TablePropertiesSuite extends AnyFunSuite {
  test("split separates reserved keys, options and properties") {
    val props = Map(
      "provider" -> "delta", "location" -> "s3://b/t", "external" -> "true", "comment" -> "c", "owner" -> "o",
      "is_managed_location" -> "true", "table_type" -> "EXTERNAL", "option.path" -> "s3://b/t",
      "option.mergeSchema" -> "true", "delta.appendOnly" -> "true")
    assert(TableProperties.split(props.asJava, "parquet") == TableSpec("delta", Some("s3://b/t"), external = true,
      Some("c"), Some("o"), Map("path" -> "s3://b/t", "mergeSchema" -> "true"), Map("delta.appendOnly" -> "true")))
  }

  test("provider defaults") {
    assert(TableProperties.split(Map.empty[String, String].asJava, "parquet").provider == "parquet")
  }

  test("partitionColumns accepts identity transforms") {
    val cols = TableProperties.partitionColumns(Array(Expressions.identity("dt"), Expressions.identity("region")))
    assert(cols == Seq("dt", "region"))
  }

  test("partitionColumns rejects other transforms") {
    val e = intercept[UnsupportedOperationException](
      TableProperties.partitionColumns(Array(Expressions.bucket(4, "id"))))
    assert(e.getMessage.contains("bucket"))
  }
}
