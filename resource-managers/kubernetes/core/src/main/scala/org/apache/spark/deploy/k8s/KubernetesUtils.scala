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
package org.apache.spark.deploy.k8s

import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{Container, ContainerBuilder, ContainerStateRunning, ContainerStateTerminated, ContainerStateWaiting, ContainerStatus, Pod, PodBuilder, Quantity}
import io.fabric8.kubernetes.client.KubernetesClient
import org.apache.commons.codec.binary.Hex
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.k8s.Config.KUBERNETES_FILE_UPLOAD_PATH
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkLauncher
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.util.{Clock, SystemClock, Utils}
import org.apache.spark.util.Utils.getHadoopFileSystem

private[spark] object KubernetesUtils extends Logging {

  private val systemClock = new SystemClock()
  private lazy val RNG = new SecureRandom()

  /**
   * Extract and parse Spark configuration properties with a given name prefix and
   * return the result as a Map. Keys must not have more than one value.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Map storing the configuration property keys and values
   */
  def parsePrefixedKeyValuePairs(
      sparkConf: SparkConf,
      prefix: String): Map[String, String] = {
    sparkConf.getAllWithPrefix(prefix).toMap
  }

  def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
  }

  /**
   * For the given collection of file URIs, resolves them as follows:
   * - File URIs with scheme local:// resolve to just the path of the URI.
   * - Otherwise, the URIs are returned as-is.
   */
  def uniqueID(clock: Clock = systemClock): String = {
    val random = new Array[Byte](3)
    synchronized {
      RNG.nextBytes(random)
    }

    val time = java.lang.Long.toHexString(clock.getTimeMillis() & 0xFFFFFFFFFFL)
    Hex.encodeHexString(random) + time
  }

  /**
   * This function builds the Quantity objects for each resource in the Spark resource
   * configs based on the component name(spark.driver.resource or spark.executor.resource).
   * It assumes we can use the Kubernetes device plugin format: vendor-domain/resource.
   * It returns a set with a tuple of vendor-domain/resource and Quantity for each resource.
   */
  def buildResourcesQuantities(
      componentName: String,
      sparkConf: SparkConf): Map[String, Quantity] = {
    val requests = ResourceUtils.parseAllResourceRequests(sparkConf, componentName)
    requests.map { request =>
      val vendorDomain = if (request.vendor.isPresent()) {
        request.vendor.get()
      } else {
        throw new SparkException(s"Resource: ${request.id.resourceName} was requested, " +
          "but vendor was not specified.")
      }
      val quantity = new Quantity(request.amount.toString)
      (KubernetesConf.buildKubernetesResourceName(vendorDomain, request.id.resourceName), quantity)
    }.toMap
  }

  /**
   * Upload files and modify their uris
   */
  def uploadAndTransformFileUris(fileUris: Iterable[String], conf: Option[SparkConf] = None)
    : Iterable[String] = {
    fileUris.map { uri =>
      resolveFileUri(uri)
    }
  }

  def resolveFileUri(uri: String): String = {
    val fileUri = Utils.resolveURI(uri)
    val fileScheme = Option(fileUri.getScheme).getOrElse("file")
    fileScheme match {
      case "local" => fileUri.getPath
      case _ => uri
    }
  }

  def parseMasterUrl(url: String): String = url.substring("k8s://".length)
}
