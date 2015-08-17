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
package org.schedoscope.scheduler.driver
import org.apache.hadoop.security.UserGroupInformation
import org.joda.time.LocalDateTime
import org.schedoscope.dsl.transformations.MapreduceTransformation
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.JobStatus.State.FAILED
import org.apache.hadoop.mapreduce.JobStatus.State.KILLED
import org.apache.hadoop.mapreduce.JobStatus.State.PREP
import org.apache.hadoop.mapreduce.JobStatus.State.RUNNING
import org.apache.hadoop.mapreduce.JobStatus.State.SUCCEEDED
import org.schedoscope.Settings
import org.schedoscope.DriverSettings
import java.security.PrivilegedAction
import org.schedoscope.scheduler.GraphiteWriter
import scala.collection.JavaConverters._
import org.apache.hadoop.mapreduce.Counter
import java.util.Date
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat

class MapreduceDriver(val ugi: UserGroupInformation) extends Driver[MapreduceTransformation] {

  val fsd = FileSystemDriver(Settings().getDriverSettings("filesystem"))

  val writer = new GraphiteWriter(Settings().graphiteHost,Settings().graphitePort)
  def driver = this
  val dateformat = new SimpleDateFormat("yyyyMMdd")

  override def transformationName = "mapreduce"
  val log = LoggerFactory.getLogger(classOf[MapreduceDriver])

  def run(t: MapreduceTransformation): DriverRunHandle[MapreduceTransformation] = try {
    ugi.doAs(new PrivilegedAction[DriverRunHandle[MapreduceTransformation]]() {
      def run(): DriverRunHandle[MapreduceTransformation] = {
        t.configure();
        
        t.directoriesToDelete.foreach(d => fsd.delete(d, true))
        log.debug(t.job.getConfiguration().toString())
        t.job.submit()
        new DriverRunHandle[MapreduceTransformation](driver, new LocalDateTime(), t, t.job)
      }
    })
  } catch {
    // when something goes wrong during submitting, we throw a driver exception -> retry
    case e: Throwable => throw DriverException("Unexpected error occurred while submitting Mapreduce job", e)
  }

  override def getDriverRunState(runHandle: DriverRunHandle[MapreduceTransformation]): DriverRunState[MapreduceTransformation] = try {
    val job = runHandle.stateHandle.asInstanceOf[Job]
    val jobId = job.getJobName
    ugi.doAs(new PrivilegedAction[DriverRunState[MapreduceTransformation]]() {
      def run(): DriverRunState[MapreduceTransformation] = {
        job.getJobState match {
          case SUCCEEDED => {logJobMetrics(job); DriverRunSucceeded[MapreduceTransformation](driver, s"Mapreduce job ${jobId} succeeded")}
          case PREP | RUNNING => DriverRunOngoing[MapreduceTransformation](driver, runHandle)
          case FAILED | KILLED => DriverRunFailed[MapreduceTransformation](driver, s"Mapreduce job ${jobId} failed", DriverException(s"Failed Mapreduce job status ${job.getJobState}"))
        }
      }
    })
  } catch {
    case e: Throwable => throw DriverException(s"Unexpected error occurred while checking run state of Mapreduce job", e)
  }

  override def runAndWait(t: MapreduceTransformation): DriverRunState[MapreduceTransformation] = try {
    ugi.doAs(new PrivilegedAction[DriverRunState[MapreduceTransformation]]() {
      def run(): DriverRunState[MapreduceTransformation] = {
        val started = new LocalDateTime()
        t.configure()
        t.directoriesToDelete.foreach(d => fsd.delete(d, true))
        t.job.waitForCompletion(true)
        getDriverRunState(new DriverRunHandle[MapreduceTransformation](driver, started, t, t.job))
      }
    })
  } catch {
    // in case there are special MR exceptions for which a retry makes sense, add these here & throw a DriverException
    case e: Throwable => DriverRunFailed[MapreduceTransformation](driver, s"Mapreduce job ${t.job.getJobName} failed", e)
  }
  
  def logJobMetrics(job:Job) = {
	   if (job.getConfiguration().get("job_date")!=null) {
		    val counters = job.getCounters().getGroup(Settings().monitoringCounterGroup).iterator()
		    val counterMap =counters.asScala.foldLeft(Map[String,Long]())((map,counter)=>map+(Settings().graphitePrefix+".hadoop-mapred."+counter.getDisplayName()->counter.getValue()))
		    val timeStamp = (dateformat.parse(job.getConfiguration().get("job_date")))
		    timeStamp.setHours(16)
		    timeStamp.setMinutes(1)
		    timeStamp.setSeconds(1)		       
		    writer.write(counterMap.asJava, (timeStamp.getTime()/1000l).toString)
	    log.debug(s"wrote ${counterMap.toString} with timestamp $timeStamp (${(timeStamp.getTime()/1000l)} to Graphite")
	   }
   }
  
  override def killRun(runHandle: DriverRunHandle[MapreduceTransformation]) = try {
    ugi.doAs(new PrivilegedAction[Unit]() {
      def run(): Unit = {
        val job = runHandle.stateHandle.asInstanceOf[Job]
        job.killJob()
      }
    })
  } catch {
    case e: Throwable => throw DriverException(s"Unexpected error occurred while killing Mapreduce job", e)
  }

}

object MapreduceDriver {
  def apply(ds: DriverSettings) = new MapreduceDriver(Settings().userGroupInformation)
}
