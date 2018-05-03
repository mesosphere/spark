/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster.mesos

import java.util.concurrent.TimeUnit
import java.util.Date

import com.codahale.metrics.{Gauge, MetricRegistry, Timer}

import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.metrics.source.Source

import org.apache.mesos.Protos.{TaskState => MesosTaskState, _}

private[mesos] class MesosClusterSchedulerSource(scheduler: MesosClusterScheduler)
  extends Source {

  override val sourceName: String = "mesos_cluster"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // PULL METRICS:
  // These gauge metrics are periodically polled/pulled by the metrics system

  metricRegistry.register(MetricRegistry.name("driver", "waiting"), new Gauge[Int] {
    override def getValue: Int = scheduler.getQueuedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("driver", "launched"), new Gauge[Int] {
    override def getValue: Int = scheduler.getLaunchedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("driver", "retry"), new Gauge[Int] {
    override def getValue: Int = scheduler.getPendingRetryDriversSize
  })

  metricRegistry.register(MetricRegistry.name("driver", "finished"), new Gauge[Int] {
    override def getValue: Int = scheduler.getFinishedDriversSize
  })

  // PUSH METRICS:
  // These counter/timer/histogram metrics are updated directly as events occur

  private val queuedCounter = metricRegistry.counter(MetricRegistry.name("driver", "waiting_count"))
  private val launchedCounter =
    metricRegistry.counter(MetricRegistry.name("driver", "launched_count"))
  private val retryingCounter = metricRegistry.counter(MetricRegistry.name("driver", "retry_count"))
  private val exceptionCounter =
    metricRegistry.counter(MetricRegistry.name("driver", "exception_count"))

  // Submission state transitions:
  // - submit():
  //     From: NULL
  //     To:   queuedDrivers
  // - offers/scheduleTasks():
  //     From: queuedDrivers and any pendingRetryDrivers scheduled for retry
  //     To:   launchedDrivers if success, or
  //           finishedDrivers(fail) if exception
  // - taskStatus/statusUpdate():
  //     From: launchedDrivers
  //     To:   finishedDrivers(success) if success (or fail and not eligible to retry), or
  //           pendingRetryDrivers if failed (and eligible to retry)
  // - pruning/retireDriver():
  //     From: finishedDrivers:
  //     To:   NULL

  private val submitToFirstLaunch =
    metricRegistry.timer(MetricRegistry.name("driver", "submit_to_first_launch"))
  private val submitToException =
    metricRegistry.timer(MetricRegistry.name("driver", "submit_to_exception"))
  private val launchToRetry = metricRegistry.timer(MetricRegistry.name("driver", "launch_to_retry"))

  // Histogram of retry counts at retry scheduling
  private val retryCount = metricRegistry.histogram(MetricRegistry.name("driver", "retry_counts"))

  // Records when a submission initially enters the launch queue.
  def recordQueuedDriver(): Unit = queuedCounter.inc()

  // Records when a submission has failed an attempt and is eligible to be retried
  def recordRetryingDriver(state: MesosClusterSubmissionState): Unit = {
    state.driverDescription.retryState.foreach(retryState => retryCount.update(retryState.retries))
    // Duration from (most recent) launch to retry
    recordTimeSince(state.startDate, launchToRetry)
    retryingCounter.inc()
  }

  // Records when a submission is launched.
  def recordLaunchedDriver(desc: MesosDriverDescription): Unit = {
    if (!desc.retryState.isDefined) {
      // Duration from submission to FIRST/NON-RETRY launch
      recordTimeSince(desc.submissionDate, submitToFirstLaunch)
    }
    launchedCounter.inc()
  }

  // Records when a submission has successfully finished, or failed and was not eligible for retry.
  def recordFinishedDriver(state: MesosClusterSubmissionState, mesosState: MesosTaskState): Unit = {
    // Record against task state, e.g. "finished_driver_count.task_lost"
    val mesosStateStr = mesosState.name().toLowerCase()

    // Duration from initial submission to finished, regardless of retries
    recordTimeSince(
      state.driverDescription.submissionDate,
      metricRegistry.timer(MetricRegistry.name("driver", "submit_to_finish", mesosStateStr)))

    // Duration from (most recent) launch to finished
    recordTimeSince(
      state.startDate,
      metricRegistry.timer(MetricRegistry.name("driver", "launch_to_finish", mesosStateStr)))

    metricRegistry.counter(
      MetricRegistry.name("driver", "finished_count", mesosStateStr)).inc()
  }

  // Records when a submission has terminally failed due to an exception at construction.
  def recordExceptionDriver(desc: MesosDriverDescription): Unit = {
    // Duration from initial submission to failed, regardless of retries
    recordTimeSince(desc.submissionDate, submitToException)
    exceptionCounter.inc()
  }

  private def recordTimeSince(date: Date, timer: Timer): Unit =
    timer.update(System.currentTimeMillis() - date.getTime(), TimeUnit.MILLISECONDS)
}
