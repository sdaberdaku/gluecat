package io.github.sdaberdaku.gluecat.it

import org.apache.spark.sql.AnalysisException
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

abstract class NamespaceIT(mode: Mode) extends SparkIT(mode) {
  private def describe(db: String): Map[String, String] =
    spark.sql(s"DESCRIBE DATABASE EXTENDED $cat.$db").collect().map(r => r.getString(0) -> r.getString(1)).toMap

  private def createParquet(db: String, t: String): Unit =
    spark.sql(s"CREATE TABLE ${tbl(db, t)} (id INT) USING parquet LOCATION '${tempDir()}'")

  test("CREATE DATABASE with comment, location and properties") {
    val loc = tempDir()
    spark.sql(s"CREATE DATABASE $cat.ns_create COMMENT 'first' LOCATION '$loc' WITH DBPROPERTIES ('k1'='v1')")
    val d = describe("ns_create")
    assert(d("Comment") == "first")
    assert(d("Location") == loc)
    assert(d("Properties").contains("(k1,v1)"))
    assert(glue.listDatabaseNames().contains("ns_create"))
  }

  test("SHOW DATABASES lists the database") {
    spark.sql(s"CREATE DATABASE $cat.ns_show")
    assert(spark.sql(s"SHOW DATABASES IN $cat").collect().map(_.getString(0)).contains("ns_show"))
  }

  test("ALTER DATABASE SET DBPROPERTIES changes a property") {
    spark.sql(s"CREATE DATABASE $cat.ns_alter WITH DBPROPERTIES ('k'='old')")
    spark.sql(s"ALTER DATABASE $cat.ns_alter SET DBPROPERTIES ('k'='new')")
    assert(describe("ns_alter")("Properties").contains("(k,new)"))
  }

  test("DROP DATABASE removes an empty database") {
    spark.sql(s"CREATE DATABASE $cat.ns_drop")
    spark.sql(s"DROP DATABASE $cat.ns_drop")
    assert(!glue.listDatabaseNames().contains("ns_drop"))
  }

  test("DROP DATABASE of a database with a table fails") {
    spark.sql(s"CREATE DATABASE $cat.ns_full")
    createParquet("ns_full", "t")
    val e = intercept[AnalysisException](spark.sql(s"DROP DATABASE $cat.ns_full"))
    assert(e.getMessage.contains("ns_full"))
    assert(glue.listTableNames("ns_full") == Seq("t"))
  }

  test("DROP DATABASE CASCADE drops the database and its Glue tables") {
    spark.sql(s"CREATE DATABASE $cat.ns_cascade")
    createParquet("ns_cascade", "t")
    spark.sql(s"DROP DATABASE $cat.ns_cascade CASCADE")
    assert(!glue.listDatabaseNames().contains("ns_cascade"))
    glue.createDatabase("ns_cascade")
    assert(glue.listTableNames("ns_cascade").isEmpty)
  }

  // In both modes Spark checks the namespace through the current catalog's namespaceExists
  // (CatalogManager.setCurrentNamespace); in Session mode the in-memory catalog has no ns_use, so success proves it.
  test("USE switches to an existing database") {
    spark.sql(s"CREATE DATABASE $cat.ns_use")
    spark.sql(s"USE $cat.ns_use")
    val current = spark.sql("SHOW CURRENT NAMESPACE").head()
    assert(current.getString(0) == cat)
    assert(current.getString(1) == "ns_use")
  }

  test("USE of a missing database fails naming it") {
    val e = intercept[AnalysisException](spark.sql(s"USE $cat.ns_missing"))
    assert(e.getMessage.contains("ns_missing"))
  }
}

@RunWith(classOf[JUnitRunner])
class NamespaceNamedIT extends NamespaceIT(Named)

@RunWith(classOf[JUnitRunner])
class NamespaceSessionIT extends NamespaceIT(Session)
