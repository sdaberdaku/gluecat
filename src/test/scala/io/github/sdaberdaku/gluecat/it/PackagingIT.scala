package io.github.sdaberdaku.gluecat.it

import java.util.jar.JarFile

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

/** The published jar holds gluecat classes only: the AWS SDK is a provided dependency, test support never ships. */
@RunWith(classOf[JUnitRunner])
class PackagingIT extends AnyFunSuite {
  private val names: Seq[String] = {
    val jar = new JarFile(System.getProperty("glue.catalog.jar"))
    try jar.entries().asScala.map(_.getName).toList finally jar.close()
  }

  private def none(description: String)(p: String => Boolean): Unit = {
    val hits = names.filter(p)
    assert(hits.isEmpty, s"$description: ${hits.take(10).mkString(", ")}")
  }

  test("jar is not empty and has a manifest") {
    assert(names.size > 50)
    assert(names.contains("META-INF/MANIFEST.MF"))
  }

  test("no SDK classes") {
    none("SDK entries")(n => n.startsWith("software/amazon/") || n.startsWith("org/reactivestreams/"))
    none("SDK service files")(_.startsWith("META-INF/services/software.amazon."))
  }

  test("no signature files and no module-info at the root") {
    none("signature files")(n => n.startsWith("META-INF/") && Seq(".SF", ".RSA", ".DSA").exists(n.endsWith))
    assert(!names.contains("module-info.class"))
  }

  test("no test support, test resources or sources") {
    none("test support")(_.startsWith("io/github/sdaberdaku/gluecat/it/"))
    none("test resources")(n => n == "log4j2.properties" || n.startsWith("fixtures/") ||
      n.startsWith("software/amazon/awssdk/global/handlers/") || n.endsWith("execution.interceptors"))
    none("Scala sources")(_.endsWith(".scala"))
  }

  test("library classes present") {
    val base = "io/github/sdaberdaku/gluecat/"
    Seq("GlueCatalog", "catalog/GlueCoreCatalog", "glue/SdkGlueApi", "format/DeltaFormatSupport",
      "format/PassThroughFormatSupport").foreach(c => assert(names.contains(base + c + ".class"), c))
  }
}
