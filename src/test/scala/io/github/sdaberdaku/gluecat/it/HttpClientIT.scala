package io.github.sdaberdaku.gluecat.it

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/** Each `http-client.type` carries the catalog's Glue and STS calls (the fixture role is assumed through it). */
@RunWith(classOf[JUnitRunner])
class HttpClientIT extends SparkIT(Named) {
  override protected def otherCatalogs: Map[String, Map[String, String]] = Map(
    "acct_apache" -> Map("http-client.type" -> "apache", "http-client.apache.max-connections" -> "8"),
    "acct_url" -> Map("http-client.type" -> "urlconnection"))

  Seq("acct_apache", "acct_url").foreach { c =>
    test(s"$c: SHOW DATABASES and CREATE DATABASE") {
      spark.sql(s"CREATE DATABASE $c.db_$c")
      assert(spark.sql(s"SHOW DATABASES IN $c").collect().map(_.getString(0)).contains(s"db_$c"))
      assert(TestGlueCatalog.recorded.asScala.exists(_._1 == c))
    }
  }
}
