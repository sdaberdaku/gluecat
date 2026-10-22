package io.github.sdaberdaku.gluecat.catalog

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogUtils, CatalogTable, CatalogTableType}
import org.apache.spark.sql.internal.HiveSerDe
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType, TimestampNTZType}

import io.github.sdaberdaku.gluecat.glue.{GlueColumn, GlueDatabaseEntry, GlueStorage, GlueTableEntry}

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

  def toGlueTable(table: CatalogTable, schemaThreshold: Int): GlueTableEntry = {
    val provider = table.provider.getOrElse(
      throw new IllegalArgumentException(s"table ${table.identifier} has no provider"))
    val uri = CatalogUtils.URIToString(table.storage.locationUri.getOrElse(
      throw new IllegalArgumentException(s"table ${table.identifier} has no location")))
    val params = table.properties ++ Map(
      "EXTERNAL" -> "TRUE", Provider -> provider, "spark.sql.create.version" -> org.apache.spark.SPARK_VERSION,
      "spark.sql.partitionProvider" -> "filesystem") ++ schemaParams(table.schema, schemaThreshold) ++
      partitionParams(table.partitionColumnNames)
    val serdeParams = table.storage.properties ++ Map("serialization.format" -> "1", "path" -> uri)

    def column(f: org.apache.spark.sql.types.StructField) =
      GlueColumn(f.name, f.dataType.catalogString, f.getComment())
    val (dataCols, partCols) = table.schema.fields.toSeq.partition(f => !table.partitionColumnNames.contains(f.name))
    def entry(extra: Map[String, String], storage: GlueStorage, keys: Seq[GlueColumn]) =
      GlueTableEntry(table.identifier.database.getOrElse(""), table.identifier.table, Some("EXTERNAL_TABLE"),
        table.comment, Some(table.owner).filter(_.nonEmpty), params ++ extra, storage, keys, None)

    val hive = HiveSerDe.sourceToSerDe(provider)
    if (provider.equalsIgnoreCase("delta")) {
      entry(Map("table_type" -> "DELTA"),
        GlueStorage(Some(uri), Some("org.apache.hadoop.mapred.SequenceFileInputFormat"),
          Some("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
          Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"), serdeParams,
          table.schema.fields.toSeq.map(column)), Nil)
    } else if (hive.isDefined && table.schema.forall(f => isHiveCompatibleType(f.dataType))) {
      entry(Map.empty, GlueStorage(Some(uri), hive.get.inputFormat, hive.get.outputFormat, hive.get.serde,
        serdeParams, dataCols.map(column)), partCols.map(column))
    } else {
      entry(Map.empty, GlueStorage(Some(uri + PlaceholderSuffix), None, None, None, serdeParams,
        Seq(GlueColumn("col", "array<string>", Some("from deserializer")))), partCols.map(column))
    }
  }

  /** False for types Hive cannot store (timestamp_ntz, variant), recursively. */
  def isHiveCompatibleType(dataType: DataType): Boolean = dataType match {
    case TimestampNTZType => false
    case ArrayType(e, _) => isHiveCompatibleType(e)
    case MapType(k, v, _) => isHiveCompatibleType(k) && isHiveCompatibleType(v)
    case s: StructType => s.forall(f => isHiveCompatibleType(f.dataType))
    // VariantType does not exist on Spark 3.5, so match by name
    case other => other.typeName != "variant"
  }

  def toNamespaceMetadata(database: GlueDatabaseEntry): java.util.Map[String, String] = {
    val m = new java.util.HashMap[String, String]()
    database.description.foreach(m.put("comment", _))
    database.locationUri.foreach(m.put("location", _))
    database.parameters.foreach { case (k, v) => m.put(k, v) }
    m
  }

  def toGlueDatabase(name: String, metadata: java.util.Map[String, String]): GlueDatabaseEntry = {
    val md = metadata.asScala.toMap
    GlueDatabaseEntry(name, md.get("comment"), md.get("location"), md - "comment" - "location")
  }

  // Same rule as CatalogTable.splitLargeTableProp: whole string when it fits, else slices of `threshold`.
  private def schemaParams(schema: StructType, threshold: Int): Map[String, String] = {
    val json = schema.json
    if (json.length <= threshold) Map(SchemaKey -> json)
    else {
      val parts = json.grouped(threshold).toSeq
      Map(NumPartsKey -> parts.size.toString) ++ parts.zipWithIndex.map { case (p, i) => s"$SchemaKey.part.$i" -> p }
    }
  }

  private def partitionParams(cols: Seq[String]): Map[String, String] =
    if (cols.isEmpty) Map.empty
    else Map(NumPartColsKey -> cols.size.toString) ++
      cols.zipWithIndex.map { case (c, i) => s"$SchemaKey.partCol.$i" -> c }
}
