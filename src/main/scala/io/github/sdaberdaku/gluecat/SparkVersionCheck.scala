package io.github.sdaberdaku.gluecat

object SparkVersionCheck {
  private val Version = """^(\d+)\.(\d+)\.(\d+).*""".r

  def requireSupported(sparkVersion: String): Unit = sparkVersion match {
    case Version(ma, mi, pa) =>
      val (major, minor, patch) = (ma.toInt, mi.toInt, pa.toInt)
      require(!(major < 3 || (major == 3 && minor < 5) || (major == 3 && minor == 5 && patch < 3)),
        s"gluecat requires Spark 3.5.3 or later, found $sparkVersion")
    case _ =>
      throw new IllegalArgumentException(s"Cannot parse Spark version '$sparkVersion'; gluecat requires Spark 3.5.3 or later")
  }
}
