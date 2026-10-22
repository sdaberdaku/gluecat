package io.github.sdaberdaku.gluecat.it

import org.apache.spark.sql.AnalysisException
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/** MiniStack holds no `default` database here. */
abstract class DefaultNamespaceIT(mode: Mode) extends SparkIT(mode) {
  private def createUnqualified(): Unit = spark.sql(s"CREATE TABLE t (id INT) USING parquet LOCATION '${tempDir()}'")

  test("unqualified names fail naming the missing default database and the glue.id") {
    spark.sql(s"USE $cat")
    val e = intercept[AnalysisException](spark.sql("SELECT * FROM t"))
    assert(e.getMessage.contains("default"))
    assert(e.getMessage.contains("111111111111"))
  }

  test("default-namespace selects the database of unqualified names") {
    startSession(Map("default-namespace" -> "main"))
    try selectsMain()
    finally startSession()
  }

  private def selectsMain(): Unit = {
    spark.sql(s"CREATE DATABASE $cat.main")
    spark.sql(s"USE $cat")
    mode match {
      case Named =>
        createUnqualified()
      case Session =>
        // For spark_catalog, Spark takes the current namespace from its built-in session catalog
        // (CatalogManager.currentNamespace), never from defaultNamespace(), so the option has no effect:
        // unqualified names stay in `default` until USE.
        val e = intercept[AnalysisException](createUnqualified())
        assert(e.getMessage.contains("default"))
        assert(e.getMessage.contains("111111111111"))
        spark.sql("USE main")
        createUnqualified()
    }
    assert(glue.getTable("main", "t").name() == "t")
  }
}

@RunWith(classOf[JUnitRunner])
class DefaultNamespaceNamedIT extends DefaultNamespaceIT(Named)

@RunWith(classOf[JUnitRunner])
class DefaultNamespaceSessionIT extends DefaultNamespaceIT(Session)
