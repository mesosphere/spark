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

package org.apache.spark.deploy.rest.mesos

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import java.util.concurrent.atomic.AtomicLong
import javax.servlet.http.HttpServletResponse

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf, SparkException}
import org.apache.spark.deploy.Command
import org.apache.spark.deploy.mesos.config._
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest.{SubmitRestProtocolException, _}
import org.apache.spark.internal.config
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.scheduler.cluster.mesos.{MesosClusterScheduler, MesosProtoUtils}
import org.apache.spark.util.Utils

/**
 * A server that responds to requests submitted by the [[RestSubmissionClient]].
 * All requests are forwarded to
 * [[org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler]].
 * This is intended to be used in Mesos cluster mode only.
 * For more details about the REST submission please refer to [[RestSubmissionServer]] javadocs.
 */
private[spark] class MesosRestServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf,
    scheduler: MesosClusterScheduler)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new MesosSubmitRequestServlet(scheduler, masterConf)
  protected override val killRequestServlet =
    new MesosKillRequestServlet(scheduler, masterConf)
  protected override val statusRequestServlet =
    new MesosStatusRequestServlet(scheduler, masterConf)
}

private[mesos] class MesosSubmitRequestServlet(
    scheduler: MesosClusterScheduler,
    conf: SparkConf)
  extends SubmitRequestServlet {

  private val DEFAULT_SUPERVISE = false
  private val DEFAULT_MEMORY = Utils.DEFAULT_DRIVER_MEM_MB // mb
  private val DEFAULT_CORES = 1.0

  private val nextDriverNumber = new AtomicLong(0)
  // For application IDs
  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)
  private def newDriverId(submitDate: Date): String =
    f"driver-${createDateFormat.format(submitDate)}-${nextDriverNumber.incrementAndGet()}%04d"

  // These defaults copied from YARN
  private val MEMORY_OVERHEAD_FACTOR = 0.10
  private val MEMORY_OVERHEAD_MIN = 384

  /**
   * Build a driver description from the fields specified in the submit request.
   *
   * This involves constructing a command that launches a mesos framework for the job.
   * This does not currently consider fields used by python applications since python
   * is not supported in mesos cluster mode yet.
   */
  // Visible for testing.
  private[rest] def buildDriverDescription(
      request: CreateSubmissionRequest): MesosDriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar 'appResource' is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class 'mainClass' is missing.")
    }
    val appArgs = Option(request.appArgs).getOrElse {
      throw new SubmitRestMissingFieldException("Application arguments 'appArgs' are missing.")
    }
    val environmentVariables = Option(request.environmentVariables).getOrElse {
      throw new SubmitRestMissingFieldException("Environment variables 'environmentVariables' " +
        "are missing.")
    }

    // Optional fields
    val sparkProperties = request.sparkProperties
    val driverDefaultJavaOptions = sparkProperties.get(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)
    val driverExtraJavaOptions = sparkProperties.get(config.DRIVER_JAVA_OPTIONS.key)
    val driverExtraClassPath = sparkProperties.get(config.DRIVER_CLASS_PATH.key)
    val driverExtraLibraryPath = sparkProperties.get(config.DRIVER_LIBRARY_PATH.key)
    val superviseDriver = sparkProperties.get(config.DRIVER_SUPERVISE.key)
    val driverMemory = sparkProperties.get(config.DRIVER_MEMORY.key)
    val driverMemoryOverhead = sparkProperties.get(config.DRIVER_MEMORY_OVERHEAD.key)
    val driverCores = sparkProperties.get(config.DRIVER_CORES.key)
    val name = request.sparkProperties.getOrElse("spark.app.name", mainClass)

    validateLabelsFormat(sparkProperties)

    val defaultConf = this.conf.getAllWithPrefix(DISPATCHER_DRIVER_DEFAULT_PREFIX).toMap
    val driverConf = new SparkConf(false)
      .setAll(defaultConf)
      .setAll(sparkProperties)

    // role propagation and enforcement
    validateRole(sparkProperties)
    getDriverRoleOrDefault(sparkProperties).foreach { role =>
      driverConf.set(ROLE.key, role)
    }

    // Construct driver description
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val defaultJavaOpts = driverDefaultJavaOptions.map(Utils.splitCommandString)
      .getOrElse(Seq.empty)
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(driverConf)
    val javaOpts = sparkJavaOpts ++ defaultJavaOpts ++ extraJavaOpts
    val command = new Command(
      mainClass, appArgs, environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverMemoryOverhead = driverMemoryOverhead.map(_.toInt).getOrElse(
      math.max((MEMORY_OVERHEAD_FACTOR * actualDriverMemory).toInt, MEMORY_OVERHEAD_MIN))
    val actualDriverCores = driverCores.map(_.toDouble).getOrElse(DEFAULT_CORES)
    val submitDate = new Date()
    val submissionId = newDriverId(submitDate)

    new MesosDriverDescription(
      name, appResource, actualDriverMemory + actualDriverMemoryOverhead, actualDriverCores,
      actualSuperviseDriver, command, driverConf.getAll.toMap, submissionId, submitDate)
  }

  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        try {
          val driverDescription = buildDriverDescription(submitRequest)
          val s = scheduler.submitDriver(driverDescription)
          s.serverSparkVersion = sparkVersion

          val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
          if (unknownFields.nonEmpty) {
            // If there are fields that the server does not know about, warn the client
            s.unknownFields = unknownFields
          }
          s
        } catch {
          case ex: SubmitRestProtocolException =>
            responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
            handleError(s"Bad request: ${ex.getMessage}")
        }
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }

  /**
   * Validates that 'spark.mesos.role' provided via spark-submit doesn't override the
   * default Dispatcher role when 'spark.mesos.dispatcher.role.enforce' is enabled.
   * In case 'spark.mesos.role' is not set for Dispatcher, no role is enforced and
   * users can submit jobs with any role.
   */
  private[mesos] def validateRole(properties: Map[String, String]): Unit = {
    properties.get(ROLE.key).filter(_.nonEmpty).foreach { driverRole =>
      conf.getOption(ROLE.key).filter(_.nonEmpty)
        .foreach { dispatcherRole =>

        val roleEnforcementEnabled = conf.get(ENFORCE_DISPATCHER_ROLE)

        if (dispatcherRole != driverRole && roleEnforcementEnabled) {
            throw new SubmitRestProtocolException(
              "Dispatcher is running with role enforcement enabled but submitted Driver" +
                s" attempts to override the default role. Enforced role: $dispatcherRole," +
                s" Driver role: $driverRole"
            )
        }
      }
    }
  }

  private[mesos] def getDriverRoleOrDefault(properties: Map[String, String]): Option[String] = {
    if (properties.get(ROLE.key).isDefined) {
       properties.get(ROLE.key)
    } else {
      conf.getOption(ROLE.key)
    }
  }

  private[mesos] def validateLabelsFormat(properties: Map[String, String]): Unit = {
    List(NETWORK_LABELS.key, TASK_LABELS.key, DRIVER_LABELS.key)
      .foreach { name =>
      properties.get(name) foreach { label =>
        try {
          MesosProtoUtils.mesosLabels(label)
        } catch {
          case _ : SparkException => throw new SubmitRestProtocolException("Malformed label in " +
            s"${name}: ${label}. Valid label format: ${name}=key1:value1,key2:value2")
        }
      }
    }
  }
}

private[mesos] class MesosKillRequestServlet(scheduler: MesosClusterScheduler, conf: SparkConf)
  extends KillRequestServlet {
  protected override def handleKill(submissionId: String): KillSubmissionResponse = {
    val k = scheduler.killDriver(submissionId)
    k.serverSparkVersion = sparkVersion
    k
  }
}

private[mesos] class MesosStatusRequestServlet(scheduler: MesosClusterScheduler, conf: SparkConf)
  extends StatusRequestServlet {
  protected override def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val d = scheduler.getDriverStatus(submissionId)
    d.serverSparkVersion = sparkVersion
    d
  }
}
