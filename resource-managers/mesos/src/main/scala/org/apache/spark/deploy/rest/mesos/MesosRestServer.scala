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
import org.apache.spark.deploy.mesos.MesosDriverDescription
import org.apache.spark.deploy.rest.{SubmitRestProtocolException, _}
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
    val driverExtraJavaOptions = sparkProperties.get("spark.driver.extraJavaOptions")
    val driverExtraClassPath = sparkProperties.get("spark.driver.extraClassPath")
    val driverExtraLibraryPath = sparkProperties.get("spark.driver.extraLibraryPath")
    val superviseDriver = sparkProperties.get("spark.driver.supervise")
    val driverMemory = sparkProperties.get("spark.driver.memory")
    val driverMemoryOverhead = sparkProperties.get("spark.driver.memoryOverhead")
    val driverCores = sparkProperties.get("spark.driver.cores")
    val name = request.sparkProperties.getOrElse("spark.app.name", mainClass)

    validateLabelsFormat(sparkProperties)

    // Construct driver description
    val defaultConf = this.conf.getAllWithPrefix("spark.mesos.dispatcher.driverDefault.").toMap
    val driverConf = new SparkConf(false)
      .setAll(defaultConf)
      .setAll(sparkProperties)

    // role propagation and enforcement
    validateRole(sparkProperties)
    getDriverRoleOrDefault(sparkProperties).foreach { role =>
      driverConf.set("spark.mesos.role", role)
    }

    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(driverConf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
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
    properties.get("spark.mesos.role").filter(_.nonEmpty).foreach { driverRole =>
      conf.getOption("spark.mesos.role").filter(_.nonEmpty)
        .foreach { dispatcherRole =>

        val roleEnforcementEnabled =
          conf.getBoolean("spark.mesos.dispatcher.role.enforce", defaultValue = false)

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
    if (properties.get("spark.mesos.role").isDefined) {
       properties.get("spark.mesos.role")
    } else {
      conf.getOption("spark.mesos.role")
    }
  }

  private[mesos] def validateLabelsFormat(properties: Map[String, String]): Unit = {
    List("spark.mesos.network.labels", "spark.mesos.task.labels", "spark.mesos.driver.labels")
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


