/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package husky
import org.apache.flink.api.scala._

object wordcount {
  def main(args: Array[String]) {
    if( args.length < 2){
      System.out.println("wordcount inputPath outputPath");
    }
    val inputPath = args(0);
    val outputPath = args(1);

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    //load
    val text = env.readTextFile(inputPath);

    //compute
    val counts = text.flatMap ( _.split("\\W+") ).map( (_, 1) ).groupBy(0).sum(1)
   
    //dump
    counts.writeAsText(outputPath);
    env.execute("wordcount example");
  }
}
