package io.github.sdaberdaku.gluecat.it

import java.sql.DriverManager

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/** Trino's Delta Lake connector reads gluecat's Glue entries from the same MiniStack store, and gluecat reads Trino's.
  * Needs the containers in docker/docker-compose.yml and TRINO_URL; without TRINO_URL every test is cancelled. */
abstract class TrinoCompatIT(mode: Mode) extends SparkIT(mode) {
  private val trinoUrl = sys.env.get("TRINO_URL")
  private def needsTrino(): Unit = assume(trinoUrl.isDefined, "TRINO_URL is not set (make compat-up, then make compat)")

  /** Runs one statement in Trino's `delta` catalog and returns every row as strings. */
  private def trino(sql: String): Seq[Seq[String]] = {
    val c = DriverManager.getConnection(trinoUrl.get, "test", null)
    try {
      val s = c.createStatement()
      if (!s.execute(sql)) Nil
      else {
        val rs = s.getResultSet
        val n = rs.getMetaData.getColumnCount
        Iterator.continually(rs).takeWhile(_.next()).map(r => (1 to n).map(i => r.getString(i)).toList).toList
      }
    } finally c.close()
  }

  private lazy val dir = compatDir()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql(s"CREATE DATABASE $cat.db")
  }

  test("Trino reads a gluecat Delta table by name") {
    needsTrino()
    spark.sql(s"CREATE TABLE ${tbl("db", "t")} (id INT, name STRING) USING delta LOCATION '$dir/t'")
    spark.sql(s"INSERT INTO ${tbl("db", "t")} VALUES (1, 'a'), (2, 'b'), (3, 'c')")
    assert(trino("SELECT * FROM delta.db.t ORDER BY id") == Seq(Seq("1", "a"), Seq("2", "b"), Seq("3", "c")))
    assert(trino("SHOW TABLES FROM delta.db").map(_.head).contains("t"))
    assert(trino("DESCRIBE delta.db.t").map(_.take(2)) == Seq(Seq("id", "integer"), Seq("name", "varchar")))
  }

  test("Trino sees rows added later") {
    needsTrino()
    spark.sql(s"INSERT INTO ${tbl("db", "t")} VALUES (4, 'd'), (5, 'e')")
    assert(trino("SELECT count(*) FROM delta.db.t") == Seq(Seq("5")))
  }

  test("Trino reads a partitioned CTAS") {
    needsTrino()
    spark.sql(s"CREATE TABLE ${tbl("db", "p")} USING delta PARTITIONED BY (dt) LOCATION '$dir/p' " +
      "AS SELECT 1 AS id, '2026-10-22' AS dt UNION ALL SELECT 2, '2026-10-23'")
    assert(trino("SELECT id FROM delta.db.p WHERE dt = '2026-10-22'") == Seq(Seq("1")))
  }

  test("gluecat reads a Delta table Trino created") {
    needsTrino()
    trino(s"CREATE TABLE delta.db.from_trino WITH (location = '$dir/from_trino') AS " +
      "SELECT * FROM (VALUES (1, CAST('a' AS varchar)), (2, CAST('b' AS varchar))) AS v (id, name)")
    val t = tbl("db", "from_trino")
    val version = glue.getTable("db", "from_trino").versionId()
    assert(spark.sql(s"SELECT * FROM $t ORDER BY id").collect().map(_.toSeq).toSeq == Seq(Seq(1, "a"), Seq(2, "b")))
    assert(spark.sql(s"DESCRIBE $t").collect().map(_.getString(0)).takeWhile(c => c.nonEmpty && !c.startsWith("#"))
      .toSeq == Seq("id", "name"))
    assert(glue.getTable("db", "from_trino").versionId() == version, "loading changed the entry")
  }
}

@RunWith(classOf[JUnitRunner])
class TrinoCompatNamedIT extends TrinoCompatIT(Named)

@RunWith(classOf[JUnitRunner])
class TrinoCompatSessionIT extends TrinoCompatIT(Session)
