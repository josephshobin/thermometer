//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.thermometer.hive

import scala.util.control.NonFatal

import scalaz._, Scalaz._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.apache.hadoop.hive.metastore.{HiveMetaHookLoader, HiveMetaStoreClient, RetryingMetaStoreClient}
import org.apache.hadoop.hive.metastore.api.Table

import org.specs2.specification.BeforeExample

import au.com.cba.omnia.thermometer.tools.HadoopSupport

/** Adds testing support for Hive by creating a `HiveConf` with a temporary path.*/
trait HiveSupport extends HadoopSupport with BeforeExample {
  lazy val hiveDir: String       = s"/tmp/hadoop/${name}/hive"
  lazy val hiveDb: String        = s"$hiveDir/hive_db"
  lazy val hiveWarehouse: String = s"$hiveDir/warehouse"
  lazy val derbyHome: String     = s"$hiveDir/derby"
  lazy val hiveConf: HiveConf    = new HiveConf <| (conf => {
    conf.setVar(METASTOREWAREHOUSE, hiveWarehouse)
  })

  // Export the warehouse path so it gets picked up when a new hive conf is instantiated somehwere else.
  System.setProperty(METASTOREWAREHOUSE.varname, hiveWarehouse)
  System.setProperty("derby.system.home", derbyHome)
  // Export the derby db file location so it is different for each test.
  System.setProperty("javax.jdo.option.ConnectionURL", s"jdbc:derby:;databaseName=$hiveDb;create=true")
  System.setProperty("hive.metastore.ds.retry.attempts", "0")

  /**
    * Run a hive operation at the start of each test.
    *
    * This prevents the Hive DB getting corrupted when the first Hive operations in the test
    * are executed in concurrently.
    */
  def before = {
    try {
      val client = RetryingMetaStoreClient.getProxy(
        hiveConf,
        new HiveMetaHookLoader() {
          override def getHook(tbl: Table) = null
        },
        classOf[HiveMetaStoreClient].getName()
      )

      try {
        client.getAllDatabases()
      } catch {
        case NonFatal(t) => throw new Exception("Failed to run hive test setup operation", t)
      } finally {
        client.close
      }
    } catch {
      case NonFatal(t) => throw new Exception("Failed to create client for hive test setup", t)
    }

  }
}
