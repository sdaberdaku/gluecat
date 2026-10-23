package io.github.sdaberdaku.gluecat

import java.io.File
import javax.xml.parsers.DocumentBuilderFactory

import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner
import org.w3c.dom.Element

/** The published POM is the module's own pom.xml (surefire runs in the module directory) with the parent POM: the AWS
  * SDK reaches users only through their own classpath. */
@RunWith(classOf[JUnitRunner])
class PublishedPomSuite extends AnyFunSuite {
  private def children(e: Element, tag: String): Seq[Element] = {
    val nodes = e.getChildNodes
    (0 until nodes.getLength).map(nodes.item).collect { case c: Element if c.getTagName == tag => c }
  }
  private def text(e: Element, tag: String): Option[String] = children(e, tag).headOption.map(_.getTextContent.trim)

  /** (artifactId, scope) of the SDK dependencies the POM declares directly, not those of dependencyManagement. */
  private def sdkDependencies(pom: String): Seq[(String, String)] = {
    val project = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new File(pom)).getDocumentElement
    children(project, "dependencies").flatMap(children(_, "dependency"))
      .filter(d => text(d, "groupId").contains("software.amazon.awssdk"))
      .map(d => (text(d, "artifactId").getOrElse(""), text(d, "scope").getOrElse("compile")))
  }

  test("glue and sts are provided, and nothing from the SDK is at compile or runtime scope") {
    val module = sdkDependencies("pom.xml")
    assert(module.contains("glue" -> "provided") && module.contains("sts" -> "provided"), module)
    val all = module ++ sdkDependencies("../../pom.xml")
    assert(all.forall { case (_, scope) => scope == "provided" || scope == "test" }, all)
  }
}
