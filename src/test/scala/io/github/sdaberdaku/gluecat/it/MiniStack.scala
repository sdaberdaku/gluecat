package io.github.sdaberdaku.gluecat.it

import java.net.{HttpURLConnection, URL}

/** The MiniStack container the integration tests talk to (Glue, STS and IAM on one port). */
object MiniStack {
  val Endpoint = "http://localhost:4566"

  /** Polls the health endpoint for up to 60 s. */
  def awaitReady(): Unit = {
    val deadline = System.nanoTime() + 60L * 1000 * 1000 * 1000
    while (!healthy()) {
      if (System.nanoTime() > deadline) throw new IllegalStateException(s"MiniStack at $Endpoint is not ready after 60 s")
      Thread.sleep(500)
    }
  }

  /** Drops all state, in every account and region. */
  def reset(): Unit = {
    val status = request("POST", "/_ministack/reset")
    if (status / 100 != 2) throw new IllegalStateException(s"MiniStack reset returned HTTP $status")
  }

  private def healthy(): Boolean =
    try request("GET", "/_ministack/health") == 200 catch { case _: java.io.IOException => false }

  private def request(method: String, path: String): Int = {
    val c = new URL(Endpoint + path).openConnection().asInstanceOf[HttpURLConnection]
    try {
      c.setRequestMethod(method)
      c.setConnectTimeout(2000)
      c.setReadTimeout(10000)
      c.getResponseCode
    } finally c.disconnect()
  }
}
