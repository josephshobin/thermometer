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

import java.io.File

import cascading.pipe.Pipe

import com.twitter.scalding.{Job, TypedPipe, Args}

import org.apache.commons.io.FileUtils

import org.apache.log4j.LogManager

import org.apache.hadoop.fs.{FileSystem, Path}

import org.specs2._
import org.specs2.execute.{Result, Failure, FailureException}
import org.specs2.matcher.{TerminationMatchers, ThrownExpectations}
import org.specs2.specification.Fragments

import scalaz.{Failure => _, _}, Scalaz._

import au.com.cba.omnia.thermometer.context.Context
import au.com.cba.omnia.thermometer.fact.Fact
import au.com.cba.omnia.thermometer.tools.{Errors, Flows, Jobs, ScaldingSupport, ExecutionSupport}
import au.com.cba.omnia.thermometer.core.Thermometer._

/** Adds functionality that makes testing scalding flows and jobs nicer.*/
abstract class ThermometerSpec extends Specification
    with TerminationMatchers
    with ThrownExpectations
    with ScalaCheck
    with ScaldingSupport
    with ExecutionSupport {
  
  implicit def PipeToVerifiable(p: Pipe) =
    new VerifiableFlow()

  implicit def TypedPipeToVerifiable[A](p: TypedPipe[A]) =
    new VerifiableFlow()
    
  implicit def JobToVerifiable(j: Job) =
    new VerifiableJob(j)

  override def map(fs: => Fragments) =
    sequential ^ isolated ^ isolate(fs)

  def isolate[A](thunk: => A): A = {
    resetFlow
    FileSystem.closeAll()
    thunk
  }

  /** Evaluate the dependency first and then evaluate test.*/
  def withDependency(dependency: => Result)(test: => Result): Result = {
    dependency
    isolate { test }
  }
  
  /** Run the test with sourceEnv being on the local hadoop path of the test.*/
  def withEnvironment(sourceEnv: Path)(test: => Result): Result = {
    val (sourceDir, targetDir) = (new File(sourceEnv.toUri.getPath), new File((dir </> "user").toUri.getPath))
    FileUtils.forceMkdir(targetDir)
    FileUtils.copyDirectory(sourceDir, targetDir)
    test
  }

  class VerifiableFlow() extends Verifiable {
    def run:Option[scalaz.\/[String,Throwable]] = Flows.runFlow(scaldingArgs, flow, mode)
  }

  class VerifiableJob(job: Job) extends Verifiable {
    def run:Option[scalaz.\/[String,Throwable]] = Jobs.runJob(job)
  }
  
  abstract class Verifiable() {
    def run:Option[scalaz.\/[String,Throwable]]
    
    def runsOk: Result = {
      val log = LogManager.getLogger(getClass)
      log.info("")
      log.info("")
      log.info(s"============================   Running test with work directory <$dir>  ============================")
      log.info("")
      log.info("")

      run match {
        case None =>
          ok
        case Some(-\/(message)) =>
          throw new FailureException(Failure(s"The pipe being tested did not complete. $message", message))
        case Some(\/-(t)) => {
          val stackTrace = Errors.renderWithStack(t)
          throw new FailureException(Failure(
            s"The pipe being tested did not complete.\n $stackTrace",
            t.getMessage,
            t.getStackTrace.toList
          ))
        }
      }
    }

    def withExpectations(f: Context => Unit): Result = {
      runsOk
      f(Context(jobConf))
      ok
    }

    def withFacts(facts: Fact*): Result = {
      runsOk
      facts.toList.map(fact => fact.run(Context(jobConf))).suml(Result.ResultMonoid)
    }
  }
}
