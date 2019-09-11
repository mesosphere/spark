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

import org.mockito.Mockito.mock

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.TestPrematureExit
import org.apache.spark.deploy.rest.{CreateSubmissionRequest, SubmitRestProtocolException}
import org.apache.spark.scheduler.cluster.mesos.MesosClusterScheduler

class MesosSubmitRequestServletSuite extends SparkFunSuite
  with TestPrematureExit {

  def buildCreateSubmissionRequest(): CreateSubmissionRequest = {
    val request = new CreateSubmissionRequest
    request.appResource = "hdfs://test.jar"
    request.mainClass = "foo.Bar"
    request.appArgs = Array.empty[String]
    request.sparkProperties = Map.empty[String, String]
    request.environmentVariables = Map.empty[String, String]
    request
  }

  test("test buildDriverDescription applies default settings from dispatcher conf to Driver") {
    val conf = new SparkConf(loadDefaults = false)

    conf.set("spark.mesos.dispatcher.driverDefault.spark.mesos.network.name", "test_network")
    conf.set("spark.mesos.dispatcher.driverDefault.spark.mesos.network.labels", "k0:v0,k1:v1")

    val submitRequestServlet = new MesosSubmitRequestServlet(
      scheduler = mock(classOf[MesosClusterScheduler]),
      conf
    )

    val request = buildCreateSubmissionRequest()
    val driverConf = submitRequestServlet.buildDriverDescription(request).conf

    assert("test_network" == driverConf.get("spark.mesos.network.name"))
    assert("k0:v0,k1:v1" == driverConf.get("spark.mesos.network.labels"))
  }

  test("test a job with malformed labels is not submitted") {
    val conf = new SparkConf(loadDefaults = false)

    val submitRequestServlet = new MesosSubmitRequestServlet(
      scheduler = mock(classOf[MesosClusterScheduler]),
      conf
    )

    val request = buildCreateSubmissionRequest()
    request.sparkProperties = Map("spark.mesos.network.labels" -> "k0,k1:v1") // malformed label

    assertThrows[SubmitRestProtocolException] {
      submitRequestServlet.buildDriverDescription(request)
    }
  }

  test("dispatcher propagates role to Drivers if 'spark.mesos.role' is not provided") {
    val dispatcherRole = "dispatcher"
    val driverRole = "driver"

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.mesos.role", dispatcherRole)

    val submitRequestServlet = new MesosSubmitRequestServlet(
      scheduler = mock(classOf[MesosClusterScheduler]),
      conf
    )

    // driver role is used when it is provided
    var request = buildCreateSubmissionRequest()
    request.sparkProperties = Map("spark.mesos.role" -> driverRole)
    var driverConf = submitRequestServlet.buildDriverDescription(request).conf
    assert(driverConf.get("spark.mesos.role") === driverRole)

    // dispatcher role is used when driver role is not provided
    request = buildCreateSubmissionRequest()
    driverConf = submitRequestServlet.buildDriverDescription(request).conf
    assert(driverConf.get("spark.mesos.role") === dispatcherRole)
  }

  test("dispatcher enforces role when 'spark.mesos.dispatcher.role.enforce' enabled") {
    val dispatcherRole = "dispatcher"
    val driverRole = "driver"

    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.mesos.role", dispatcherRole)
    conf.set("spark.mesos.dispatcher.role.enforce", "true")

    val submitRequestServlet = new MesosSubmitRequestServlet(
      scheduler = mock(classOf[MesosClusterScheduler]),
      conf
    )

    // dispatcher role is used by default
    var request = buildCreateSubmissionRequest()
    var driverConf = submitRequestServlet.buildDriverDescription(request).conf
    assert(driverConf.get("spark.mesos.role") === dispatcherRole)

    // if both driver and dispatcher use the same role the request should be accepted
    request = buildCreateSubmissionRequest()
    request.sparkProperties = Map("spark.mesos.role" -> dispatcherRole)
    driverConf = submitRequestServlet.buildDriverDescription(request).conf
    assert(driverConf.get("spark.mesos.role") === dispatcherRole)

    // if driver specifies a different role and enforcement is enabled the request should fail
    request = buildCreateSubmissionRequest()
    request.sparkProperties = Map("spark.mesos.role" -> driverRole)
    assertThrows[SubmitRestProtocolException] {
      submitRequestServlet.buildDriverDescription(request)
    }
  }
}
