package io.github.sdaberdaku.gluecat

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BuildSmokeSuite extends AnyFunSuite {
  test("Spark on the test classpath matches the module's Spark minor") {
    assert(org.apache.spark.SPARK_VERSION.startsWith(System.getProperty("spark.minor") + "."))
  }
  test("Delta is on the test classpath") {
    Class.forName("org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }
}
