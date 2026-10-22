package io.github.sdaberdaku.gluecat.it

import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** How the catalog under test is configured: as the named catalog `acct_a`, or as `spark_catalog`. */
sealed trait Mode
object Named extends Mode
object Session extends Mode

/** One local Spark session per suite, on a freshly reset MiniStack, account 111111111111. */
abstract class SparkIT(mode: Mode) extends AnyFunSuite with BeforeAndAfterAll {
  val cat: String = mode match {
    case Named => "acct_a"
    case Session => "spark_catalog"
  }
  def tbl(db: String, t: String): String = s"$cat.$db.$t"

  val glue = new GlueInspector("111111111111")
  private val root: Path = Files.createTempDirectory("gluecat-it")
  protected var spark: SparkSession = _

  /** A new empty directory, as a `file:` URI in the form Spark qualifies locations to. */
  def tempDir(): String = "file:" + Files.createTempDirectory(root, "t").toAbsolutePath

  /** Every message in the cause chain, for errors Spark wraps. */
  def fullMessage(e: Throwable): String =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")

  /** Stops the current session and starts one with `catalogOptions` added to the catalog's options. */
  protected def startSession(catalogOptions: Map[String, String] = Map.empty): Unit = {
    stopSession()
    val b = SparkSession.builder().master("local[2]").appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.warehouse.dir", s"$root/spark-warehouse")
      .config("spark.sql.catalogImplementation", "in-memory")
    if (mode == Named) b.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    val options = Map(
      "glue.id" -> "111111111111",
      "client.region" -> "eu-central-1",
      "glue.endpoint" -> MiniStack.Endpoint,
      "warehouse" -> s"file:$root/wh") ++ catalogOptions
    b.config(s"spark.sql.catalog.$cat", classOf[TestGlueCatalog].getName)
    options.foreach { case (k, v) => b.config(s"spark.sql.catalog.$cat.$k", v) }
    spark = b.getOrCreate()
  }

  private def stopSession(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    spark = null
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    MiniStack.awaitReady()
    MiniStack.reset()
    startSession()
  }

  override def afterAll(): Unit =
    try {
      stopSession()
      glue.close()
      Files.walk(root).iterator().asScala.toList.reverse.foreach(p => Files.deleteIfExists(p))
    } finally super.afterAll()
}
