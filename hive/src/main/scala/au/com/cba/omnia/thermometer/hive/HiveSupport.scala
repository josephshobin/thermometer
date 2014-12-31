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

import scalaz._, Scalaz._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._

import au.com.cba.omnia.thermometer.tools.HadoopSupport

/** Adds testing support for Hive by creating a `HiveConf` with a temporary path.*/
trait HiveSupport extends HadoopSupport {
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
}
