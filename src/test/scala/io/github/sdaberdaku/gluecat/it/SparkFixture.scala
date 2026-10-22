package io.github.sdaberdaku.gluecat.it

import java.nio.file.{Files, Path, Paths}
import java.nio.file.attribute.PosixFilePermissions

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

  /** A new empty directory under /tmp/gluecat-compat, which the compatibility containers (docker/docker-compose.yml)
    * mount at the same path; world-writable because they run as other users. Not deleted afterwards: the containers
    * write files there that this JVM cannot remove. */
  def compatDir(): String = {
    val dir = Files.createTempDirectory(Paths.get("/tmp/gluecat-compat"), getClass.getSimpleName)
    "file:" + Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxrwxrwx")).toAbsolutePath
  }

  /** Every message in the cause chain, for errors Spark wraps. */
  def fullMessage(e: Throwable): String =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")

  /** More `TestGlueCatalog`s for every session, by name, with their options on top of the shared ones; a
    * `spark_catalog` entry here replaces the Named mode's DeltaCatalog. */
  protected def otherCatalogs: Map[String, Map[String, String]] = Map.empty

  /** Stops the current session and starts one with `catalogOptions` added to the catalog's options. */
  protected def startSession(catalogOptions: Map[String, String] = Map.empty): Unit = {
    stopSession()
    TestGlueCatalog.reset()
    val b = SparkSession.builder().master("local[2]").appName(getClass.getSimpleName)
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.warehouse.dir", s"$root/spark-warehouse")
      .config("spark.sql.catalogImplementation", "in-memory")
    if (mode == Named) b.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    (otherCatalogs + (cat -> catalogOptions)).foreach { case (name, own) =>
      val shared = Map(
        "glue.id" -> "111111111111",
        "client.region" -> "eu-central-1",
        "glue.endpoint" -> MiniStack.Endpoint,
        "warehouse" -> s"file:$root/wh/$name")
      b.config(s"spark.sql.catalog.$name", classOf[TestGlueCatalog].getName)
      (shared ++ own).foreach { case (k, v) => b.config(s"spark.sql.catalog.$name.$k", v) }
    }
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
    try stopSession()
    finally try glue.close()
    finally try deleteRecursively(root)
    finally super.afterAll()

  private def deleteRecursively(dir: Path): Unit = {
    val paths = Files.walk(dir)
    try paths.iterator().asScala.toList.reverse.foreach(p => Files.deleteIfExists(p))
    finally paths.close()
  }
}
