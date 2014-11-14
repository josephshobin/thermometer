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

package au.com.cba.omnia.thermometer.tools

import scala.util.{Try, Success, Failure}

import scalaz._, Scalaz._

import com.twitter.scalding._

import org.apache.hadoop.mapred.JobConf

import org.specs2.Specification
import org.specs2.execute.{Result, Success => SpecsSuccess, Failure => SpecsFailure, FailureException}
import org.specs2.execute.StandardResults.success

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.fact.Fact

/** Adds testing support for scalding execution monad by setting up a test `Config` and `Mode`.*/
trait ExecutionSupport extends FieldConversions with HadoopSupport { self: Specification =>
  /** Executes the provided execution with an optional map of arguments. */
  def execute[T](execution: Execution[T], args: Map[String, List[String]] = Map.empty): Try[T] = {
    println("")
    println("")
    println(s"============================   Running execution test with work directory <$dir>  ============================")
    println("")
    println("")

    val mode   = Hdfs(false, jobConf)
    val a      = Mode.putMode(mode, new Args(args + (("hdfs", List.empty))))
    val config =
      Config.hadoopWithDefaults(jobConf)
        .setArgs(a)

    Executions.runExecution[T](config, mode, execution)
  }

  /**
    * Checks that the provided execution executed successfully.
    * 
    * It ignores the result of the execution.
    * Takes an optional map of arguments.
    */
  def executesOk(execution: Execution[_], args: Map[String, List[String]] = Map.empty): Result = {
    execute(execution, args) match {
      case Success(x) => SpecsSuccess()
      case Failure(t) =>
        throw FailureException(SpecsFailure(s"Execution failed: ${t.toString}", "", t.getStackTrace.toList))
    }
  }

  /**
    * Checks that the provided execution executed successfully and returns the result
    * 
    * Takes an optional map of arguments.
    */
  def executesSuccessfully[T](execution: Execution[T], args: Map[String, List[String]] = Map.empty): T = {
    execute(execution, args) match {
      case Success(x) => x
      case Failure(t) =>
        throw FailureException(SpecsFailure(s"Execution failed: ${t.toString}", "", t.getStackTrace.toList))
    }
  }


  /** Verifies the provided list of expections in the context of this test. */
  def expectations(f: Context => Unit): Result = {
    f(Context(jobConf))
    success
  }

  /** Verifies the provided list of Facts in the context of this test. */
  def facts(facts: Fact*): Result =
    facts.toList.map(fact => fact.run(Context(jobConf))).suml(Result.ResultMonoid)
}
