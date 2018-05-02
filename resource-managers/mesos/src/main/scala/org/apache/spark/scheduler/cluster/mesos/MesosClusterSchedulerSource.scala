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

private[mesos] class MesosClusterSchedulerSource(scheduler: MesosClusterScheduler)
  extends Source {

  override val sourceName: String = "mesos_cluster"
  override val metricRegistry: MetricRegistry = new MetricRegistry()

  // PULL METRICS:
  // These gauge metrics are periodically polled/pulled by the metrics system

  metricRegistry.register(MetricRegistry.name("waitingDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getQueuedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("launchedDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getLaunchedDriversSize
  })

  metricRegistry.register(MetricRegistry.name("retryDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getPendingRetryDriversSize
  })

  metricRegistry.register(MetricRegistry.name("finishedDrivers"), new Gauge[Int] {
    override def getValue: Int = scheduler.getFinishedDriversSize
  })

  // PUSH METRICS:
  // These counter/timer/histogram metrics are updated directly as events occur

  private val queuedCounter = metricRegistry.counter(MetricRegistry.name("waitingDriverCount"))
  private val launchedCounter = metricRegistry.counter(MetricRegistry.name("launchedDriverCount"))
  private val retryingCounter = metricRegistry.counter(MetricRegistry.name("retryDriverCount"))
  private val finishedCounter = metricRegistry.counter(MetricRegistry.name("finishedDriverCount"))
  private val failedCounter = metricRegistry.counter(MetricRegistry.name("failedDriverCount"))

  // Submission state transitions:
  // - submit():
  //     From: NULL
  //     To:   queuedDrivers
  // - offers/scheduleTasks():
  //     From: queuedDrivers and any pendingRetryDrivers scheduled for retry
  //     To:   launchedDrivers if success, or finishedDrivers(fail) if failed
  // - taskStatus/statusUpdate():
  //     From: launchedDrivers
  //     To:   finishedDrivers(success) if success, or pendingRetryDrivers if failed
  // - pruning/retireDriver():
  //     From: finishedDrivers:
  //     To:   NULL

  // Duration from submission to FIRST/NON-RETRY launch
  private val submitToFirstLaunch = metricRegistry.timer(MetricRegistry.name("submitToFirstLaunch"))
  // Duration from initial submission to finished, regardless of retries
  private val submitToFinish = metricRegistry.timer(MetricRegistry.name("submitToFinish"))
  // Duration from initial submission to failed, regardless of retries
  private val submitToFail = metricRegistry.timer(MetricRegistry.name("submitToFail"))

  // Duration from (most recent) launch to finished
  private val launchToFinish = metricRegistry.timer(MetricRegistry.name("launchToFinish"))
  // Duration from (most recent) launch to retry
  private val launchToRetry = metricRegistry.timer(MetricRegistry.name("launchToRetry"))

  // Histogram of retry counts at retry scheduling
  private val retryCount = metricRegistry.histogram(MetricRegistry.name("retryCount"))

  // Records when a submission initially enters the launch queue.
  def recordQueuedDriver(): Unit = queuedCounter.inc()

  // Records when a submission has failed an attempt and is marked for retrying.
  def recordRetryingDriver(state: MesosClusterSubmissionState): Unit = {
    state.driverDescription.retryState.foreach(retryState => retryCount.update(retryState.retries))
    recordTimeSince(launchToRetry, state.startDate)
    retryingCounter.inc()
  }

  // Records when a submission is launched.
  def recordLaunchedDriver(desc: MesosDriverDescription): Unit = {
    if (!desc.retryState.isDefined) {
      recordTimeSince(submitToFirstLaunch, desc.submissionDate)
    }
    launchedCounter.inc()
  }

  // Records when a submission has successfully finished.
  def recordFinishedDriver(state: MesosClusterSubmissionState): Unit = {
    recordTimeSince(submitToFinish, state.driverDescription.submissionDate)
    recordTimeSince(launchToFinish, state.startDate)
    finishedCounter.inc()
  }

  // Records when a submission has terminally failed due to an exception.
  def recordFailedDriver(desc: MesosDriverDescription): Unit = {
    recordTimeSince(submitToFail, desc.submissionDate)
    failedCounter.inc()
  }

  private def recordTimeSince(timer: Timer, date: Date): Unit =
    timer.update(System.currentTimeMillis() - date.getTime(), TimeUnit.MILLISECONDS)
}
