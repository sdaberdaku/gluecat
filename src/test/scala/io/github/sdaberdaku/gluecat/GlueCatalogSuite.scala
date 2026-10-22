package io.github.sdaberdaku.gluecat

import scala.collection.JavaConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GlueCatalogSuite extends AnyFunSuite {
  test("initialize failures name the catalog") {
    val options = new CaseInsensitiveStringMap(Map("default-namespace" -> "a.b").asJava)
    val e = intercept[IllegalArgumentException](new GlueCatalog().initialize("acct_x", options))
    assert(e.getMessage.startsWith("gluecat catalog 'acct_x': "))
    assert(e.getMessage.contains("default-namespace must be a single level"))
  }
}
