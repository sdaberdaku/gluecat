package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.expressions.Transform

final case class TableSpec(provider: String, location: Option[String], external: Boolean, comment: Option[String],
    owner: Option[String], options: Map[String, String], properties: Map[String, String])

object TableProperties {
  val ReservedKeys: Set[String] =
    Set("provider", "location", "external", "is_managed_location", "comment", "owner", "table_type")

  private val OptionPrefix = "option."

  def split(properties: java.util.Map[String, String], defaultProvider: String): TableSpec = {
    val all = properties.asScala.toMap
    TableSpec(
      provider = all.getOrElse("provider", defaultProvider),
      location = all.get("location"),
      external = all.get("external").exists(_.equalsIgnoreCase("true")),
      comment = all.get("comment"),
      owner = all.get("owner"),
      options = all.collect { case (k, v) if k.startsWith(OptionPrefix) => k.stripPrefix(OptionPrefix) -> v },
      properties = all.filter { case (k, _) => !ReservedKeys.contains(k) && !k.startsWith(OptionPrefix) })
  }

  def partitionColumns(transforms: Array[Transform]): Seq[String] = transforms.toSeq.map { t =>
    val fields = if (t.name() == "identity" && t.references().length == 1) t.references()(0).fieldNames() else null
    if (fields == null || fields.length != 1) {
      throw new UnsupportedOperationException(s"Unsupported partition transform: $t (only identity is supported)")
    }
    fields(0)
  }
}
