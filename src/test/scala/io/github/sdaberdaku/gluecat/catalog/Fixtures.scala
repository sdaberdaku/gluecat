package io.github.sdaberdaku.gluecat.catalog

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.github.sdaberdaku.gluecat.glue.GlueTableEntry

object Fixtures {
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def table(name: String): GlueTableEntry = {
    val in = getClass.getResourceAsStream(s"/fixtures/$name.json")
    require(in != null, s"missing fixture $name")
    try mapper.readValue(in, classOf[GlueTableEntry]) finally in.close()
  }
}
