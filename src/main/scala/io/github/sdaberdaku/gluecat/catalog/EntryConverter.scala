package io.github.sdaberdaku.gluecat.catalog

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogUtils, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import io.github.sdaberdaku.gluecat.glue.GlueTableEntry

/** Pure conversion between Glue table entries and Spark's CatalogTable. */
object EntryConverter {
  val HiddenParameters: Set[String] = Set("EXTERNAL", "table_type", "transient_lastDdlTime")
  val PlaceholderSuffix = "-__PLACEHOLDER__"

  private val Provider = "spark.sql.sources.provider"
  private val SchemaKey = "spark.sql.sources.schema"
  private val NumPartsKey = s"$SchemaKey.numParts"
  private val NumPartColsKey = s"$SchemaKey.numPartCols"

  def location(entry: GlueTableEntry): Option[String] =
    entry.storage.serdeParameters.get("path")
      .orElse(entry.storage.location.filterNot(_.endsWith(PlaceholderSuffix)))

  def toCatalogTable(catalogName: String, entry: GlueTableEntry): CatalogTable = {
    val id = s"$catalogName.${entry.database}.${entry.name}"
    val params = entry.parameters
    def unsupported(reason: String) = new UnsupportedTableException(id, reason)
    def malformed(reason: String) = new MalformedEntryException(id, reason)

    if (entry.tableType.contains("VIRTUAL_VIEW")) throw unsupported("Glue views are not supported (view)")
    if (params.get("table_type").exists(_.equalsIgnoreCase("ICEBERG"))) {
      throw unsupported("Iceberg tables are not loaded by this catalog; register the Iceberg catalog " +
        "(org.apache.iceberg.spark.SparkCatalog with catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog) for them")
    }
    val provider = params.getOrElse(Provider,
      throw unsupported(s"not a Spark data source table (missing $Provider)"))
    val uri = location(entry).getOrElse(throw malformed("no table location (SerDe path or storage location)"))

    CatalogTable(
      identifier = TableIdentifier(entry.name, Some(entry.database), Some(catalogName)),
      tableType = if (entry.tableType.contains("MANAGED_TABLE")) CatalogTableType.MANAGED
      else CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat(Some(CatalogUtils.stringToURI(uri)), entry.storage.inputFormat, entry.storage.outputFormat,
        entry.storage.serdeLibrary, compressed = false,
        entry.storage.serdeParameters - "path" - "serialization.format"),
      schema = schema(params, malformed),
      provider = Some(provider),
      partitionColumnNames = partitionColumns(params, malformed),
      comment = entry.description,
      owner = entry.owner.getOrElse(""),
      createVersion = params.getOrElse("spark.sql.create.version", ""),
      properties = params.filter { case (k, _) => !HiddenParameters(k) && !k.startsWith("spark.sql.") },
      tracksPartitionsInCatalog = params.get("spark.sql.partitionProvider").contains("catalog"))
  }

  private def schema(params: Map[String, String], malformed: String => Exception): StructType = {
    val json = params.get(SchemaKey).orElse(params.get(NumPartsKey).map { n =>
      (0 until parseCount(NumPartsKey, n, malformed)).map { i =>
        params.getOrElse(s"$SchemaKey.part.$i", throw malformed(s"schema part $SchemaKey.part.$i is missing"))
      }.mkString
    })
    json.map { j =>
      try DataType.fromJson(j).asInstanceOf[StructType]
      catch { case NonFatal(e) => throw malformed(s"cannot parse the stored schema: ${e.getMessage}") }
    }.getOrElse(new StructType())
  }

  private def partitionColumns(params: Map[String, String], malformed: String => Exception): Seq[String] =
    params.get(NumPartColsKey).toSeq.flatMap { n =>
      (0 until parseCount(NumPartColsKey, n, malformed)).map { i =>
        params.getOrElse(s"$SchemaKey.partCol.$i", throw malformed(s"partition column $SchemaKey.partCol.$i is missing"))
      }
    }

  private def parseCount(key: String, value: String, malformed: String => Exception): Int =
    try value.toInt catch { case _: NumberFormatException => throw malformed(s"$key is not a number: $value") }
}
