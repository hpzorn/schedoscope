package org.schedoscope.scheduler

import com.typesafe.config._
import com.timgroup.statsd.{NonBlockingStatsDClient, StatsDClient}
import java.net.Socket
import java.util.Map
import scala.collection.JavaConversions._
import java.io.Writer
import java.io.OutputStream
import java.io.IOException

trait Metrics {

  val conf = ConfigFactory.load()
  val statsdHost = conf.getString("kamon.statsd.hostname")
  val statsdPort = conf.getInt("kamon.statsd.port")
  val prefix = conf.getString("kamon.statsd.simple-metric-key-generator.application")
  val hostname = java.net.InetAddress.getLocalHost.getHostName.replace('.','_')
  var globalPrefix = s"$prefix.$hostname"

  def createNamingScheme(group: String, instance: String) =
    (metric: String) => s"$group.$instance.$metric"

  abstract class MetricsProducer() {
    val statsd: StatsDClient = new NonBlockingStatsDClient(globalPrefix, statsdHost, statsdPort)
    val group: String
    val instance: String

    def scheme = createNamingScheme(group, instance)


    def createCounter(name: String) = {
      () => statsd.incrementCounter(scheme(name))
    }

    def createValueGauge(name: String) =
      (value: Long) => statsd.recordGaugeValue(scheme(name), value)
      
  }

  class MapreduceMetrics(instanceName: String) extends MetricsProducer {
    override val group: String = "mapr"
    override val instance: String = instanceName

    def incrementReceived = createCounter("events-received")

    def incrementTimeout = createCounter("timeouts")

  }

  class PumpMetrics(instanceName: String) extends MetricsProducer {
    override val group: String = "wt-pump"
    override val instance: String = instanceName

    def incrementSent = createCounter("events-sent")

    def recordRedisResponses(value: Long) = createValueGauge("responding-redis")(value)
  }

}

class GraphiteWriter(private var host: String, private var port: Int)  {

   def write(counts: Map[_, _], timestamp: String) {
    var outputSocket: Socket = null
    var out: OutputStream = null
    try {
      outputSocket = new Socket(host, port)
      out = outputSocket.getOutputStream
      for (key <- counts.keySet) {
        val output = (key + " " + counts.get(key) + " " + timestamp + "\n")
        out.write(output.getBytes)
      }
      out.flush()
    } catch {
      case e: IOException => println(e.getMessage)
    } finally {
      if (out != null) {
        try {
          out.close()
        } catch {
          case e: IOException => println(e.getMessage)
        }
      }
    }
  }
}
