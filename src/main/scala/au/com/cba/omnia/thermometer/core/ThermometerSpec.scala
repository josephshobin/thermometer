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

package au.com.cba.omnia.thermometer
package core

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import au.com.cba.omnia.thermometer.fact._
import au.com.cba.omnia.thermometer.tools._
import au.com.cba.omnia.thermometer.context._
import com.twitter.scalding._

import org.specs2._
import org.specs2.matcher._
import org.specs2.execute._
import org.specs2.specification.Fragments

import scalaz.{Failure => _, _}, Scalaz._
import scala.util.control.NonFatal


abstract class ThermometerSpec extends Specification
    with TerminationMatchers
    with ThrownExpectations
    with ScalaCheck
    with ScaldingSupport {

  override def map(fs: => Fragments) =
    sequential ^ isolated ^ fs

  implicit def PipeToVerifiable(p: Pipe) =
    Verifiable()

  implicit def TypedPipeToVerifiable[A](p: TypedPipe[A]) =
    Verifiable()

  def isolate[A](thunk: => A): A = {
    resetFlow
    thunk
  }

  def withDependency(dependency: => Result)(test: => Result): Result = {
    dependency
    isolate { test }
  }

  case class Verifiable() {
    def runsOk: Result = {
      println("")
      println("")
      println(s"============================   Running flow with work directory <$dir>  ============================")
      println("")
      println("")

      Flows.runFlow(scaldingArgs, flow, mode) match {
        case None =>
          ok
        case Some(-\/(message)) =>
          throw new FailureException(Failure("The pipe being tested did not complete.", message))
        case Some(\/-(t)) =>
          throw new FailureException(Failure("The pipe being tested did not complete.", Errors.renderWithStack(t), t.getStackTrace.toList))
      }
    }

    def withExpectations(f: Context => Unit): Result = {
      runsOk
      f(Context(conf))
      ok
    }

    def withFacts(facts: Fact*): Result = {
      runsOk
      facts.toList.map(fact => fact.run(Context(conf))).suml(Result.ResultMonoid)
    }
  }
}
