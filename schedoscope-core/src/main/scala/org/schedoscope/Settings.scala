/**
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.schedoscope

import java.net.URLClassLoader
import java.nio.file.Paths
import java.util.Calendar
import java.util.concurrent.TimeUnit
import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap
import scala.concurrent.duration.Duration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.schedoscope.scheduler.driver.FileSystemDriver.fileSystem
import org.schedoscope.dsl.Parameter.p
import org.schedoscope.dsl.Transformation
import org.schedoscope.dsl.views.DateParameterizationUtils
import com.typesafe.config.Config
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import org.schedoscope.scheduler.driver.Driver
import org.schedoscope.dsl.views.ViewUrlParser.ParsedViewAugmentor

class SettingsImpl(val config: Config) extends Extension {
  val system = Settings.actorSystem

  private val driverSettings: HashMap[String, DriverSettings] = HashMap[String, DriverSettings]()

  lazy val env = config.getString("schedoscope.app.environment")

  lazy val earliestDay = {
    val conf = config.getString("schedoscope.scheduler.earliestDay")
    val Array(year, month, day) = conf.split("-")
    DateParameterizationUtils.parametersToDay(p(year), p(month), p(day))
  }

  lazy val latestDay = {
    val conf = config.getString("schedoscope.scheduler.latestDay")
    if (conf == "now") {
      val now = Calendar.getInstance()
      now.set(Calendar.HOUR_OF_DAY, 0)
      now.set(Calendar.MINUTE, 0)
      now.set(Calendar.SECOND, 0)
      now.set(Calendar.MILLISECOND, 0)
      now
    } else {
      val Array(year, month, day) = conf.split("-")
      DateParameterizationUtils.parametersToDay(p(year), p(month), p(day))
    }
  }

  lazy val webserviceTimeOut: Duration =
    Duration(config.getDuration("schedoscope.webservice.timeout", TimeUnit.MILLISECONDS),
      TimeUnit.MILLISECONDS)

  lazy val host = config.getString("schedoscope.webservice.host")

  lazy val port = config.getInt("schedoscope.webservice.port")

  lazy val webResourcesDirectory = config.getString("schedoscope.webservice.resourceDirectory")

  lazy val restApiConcurrency = config.getInt("schedoscope.webservice.concurrency")

  lazy val jdbcUrl = config.getString("schedoscope.metastore.jdbcUrl")

  lazy val kerberosPrincipal = config.getString("schedoscope.kerberos.principal")

  lazy val metastoreUri = config.getString("schedoscope.metastore.metastoreUri")

  lazy val parsedViewAugmentorClass = config.getString("schedoscope.app.parsedViewAugmentorClass")

  def viewAugmentor = Class.forName(parsedViewAugmentorClass).newInstance().asInstanceOf[ParsedViewAugmentor]

  lazy val availableTransformations = config.getObject("schedoscope.transformations")

  lazy val hadoopConf = new Configuration(true)

  lazy val transformationVersioning = config.getBoolean("schedoscope.versioning.transformations")

  lazy val jobTrackerOrResourceManager = {
    val yarnConf = new YarnConfiguration(hadoopConf)
    if (yarnConf.get("yarn.resourcemanager.address") == null)
      config.getString("schedoscope.hadoop.resourceManager")
    else
      yarnConf.get("yarn.resourcemanager.address")
  }

  lazy val nameNode = if (hadoopConf.get("fs.defaultFS") == null)
    config.getString("schedoscope.hadoop.nameNode")
  else
    hadoopConf.get("fs.defaultFS")

  lazy val filesystemTimeout = getDriverSettings("filesystem").timeout
  lazy val schemaTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.schema", TimeUnit.SECONDS), TimeUnit.SECONDS)
  lazy val statusListAggregationTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.statusListAggregation", TimeUnit.SECONDS), TimeUnit.SECONDS)
  lazy val viewManagerResponseTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.viewManagerResponse", TimeUnit.SECONDS), TimeUnit.SECONDS)
  lazy val completitionTimeout = Duration.create(config.getDuration("schedoscope.scheduler.timeouts.completion", TimeUnit.SECONDS), TimeUnit.SECONDS)

  lazy val retries = config.getInt("schedoscope.action.retry")

  lazy val metastoreConcurrency = config.getInt("schedoscope.metastore.concurrency")
  lazy val metastoreWriteBatchSize = config.getInt("schedoscope.metastore.writeBatchSize")
  lazy val metastoreReadBatchSize = config.getInt("schedoscope.metastore.readBatchSize")

  lazy val graphiteHost=config.getString("schedoscope.monitoring.graphiteHost")
  lazy val graphitePort= config.getInt("schedoscope.monitoring.graphitePort")
  lazy val graphitePrefix= config.getString("schedoscope.monitoring.graphitePrefix")
  lazy val monitoringCounterGroup= config.getString("schedoscope.monitoring.counterGroup")
  lazy val enabled = config.getBoolean("schedoscope.monitoring.enabled")
  lazy val userGroupInformation = {
    UserGroupInformation.setConfiguration(hadoopConf)
    val ugi = UserGroupInformation.getCurrentUser()
    ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS)
    ugi.reloginFromKeytab();
    ugi
  }

  def getDriverSettings(d: Any with Driver[_]): DriverSettings = {
    getDriverSettings(d.transformationName)
  }

  def getDriverSettings[T <: Transformation](t: T): DriverSettings = {
    val name = t.getClass.getSimpleName.toLowerCase.replaceAll("transformation", "").replaceAll("\\$", "")
    getDriverSettings(name)
  }

  def getDriverSettings(n: String): DriverSettings = {
    if (!driverSettings.contains(n)) {
      val confName = "schedoscope.transformations." + n
      driverSettings.put(n, new DriverSettings(config.getConfig(confName), n))
    }

    driverSettings(n)
  }

  def getTransformationSetting(typ: String, setting: String) = {
    val confName = s"schedoscope.transformations.${typ}.transformation.${setting}"
    config.getString(confName)
  }

}

object Settings extends ExtensionId[SettingsImpl] with ExtensionIdProvider {
  val actorSystem = ActorSystem("schedoscope")

  override def lookup = Settings

  override def createExtension(system: ExtendedActorSystem) =
    new SettingsImpl(system.settings.config)

  override def get(system: ActorSystem): SettingsImpl = super.get(system)

  def apply() = {
    super.apply(actorSystem)
  }
}

class DriverSettings(val config: Config, val name: String) {
  lazy val location = Settings().nameNode + config.getString("location") + Settings().env + "/"
  lazy val libDirectory = config.getString("libDirectory")
  lazy val concurrency = config.getInt("concurrency")
  lazy val unpack = config.getBoolean("unpack")
  lazy val url = config.getString("url")
  lazy val timeout = Duration.create(config.getDuration("timeout", TimeUnit.SECONDS), TimeUnit.SECONDS)

  lazy val libJars = {
    val fromLibDir = libDirectory
      .split(",")
      .toList
      .filter(!_.trim.equals(""))
      .map(p => { if (!p.endsWith("/")) s"file://${p.trim}/*" else s"file://${p.trim}*" })
      .flatMap(dir => {
        fileSystem(dir, Settings().hadoopConf).globStatus(new Path(dir))
          .map(stat => stat.getPath.toString)
      })

    val fromClasspath = this.getClass.getClassLoader
      .asInstanceOf[URLClassLoader]
      .getURLs
      .map(el => el.toString)
      .distinct
      .filter(_.endsWith(s"-${name}.jar"))
      .toList

    (fromLibDir ++ fromClasspath).toList
  }

  lazy val libJarsHdfs = {
    if (unpack)
      List[String]()
    else {
      libJars.map(lj => location + "/" + Paths.get(lj).getFileName.toString)
    }
  }
}

