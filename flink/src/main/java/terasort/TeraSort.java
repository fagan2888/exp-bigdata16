/*
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

package terasort;

import org.apache.flink.api.java.*;
import org.apache.flink.api.java.tuple.Tuple2;
/* import org.apache.flink.api.java.hadoop.mapred.OutputFormat; */
/* import org.apache.flink.api.java.hadoop.mapreduce.*; */
//can not find RichOutputFormat
import org.apache.flink.api.common.operators.Order;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;
import java.lang.Exception;
import java.lang.Integer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import org.apache.flink.api.common.io.*;
import org.apache.flink.api.java.io.*;
/* import org.apache.flink.core.fs.Path; */

public final class TeraSort {

    public static void main(String[] args) throws IOException, Exception {
        if( args.length < 2 ){
            System.out.println("usage: TeraSort [inputDir] [outputDir]");
            return ;
        }
        long startTime = System.currentTimeMillis();
        String inputPath = args[0];
        String outputPath = args[1];
        int parallelism = 432;
        if( args.length > 2) parallelism = Integer.parseInt( args[2] );

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSet<Tuple2<Text, Text>> dataset = env.readHadoopFile(new HadoopTeraInputFormat(), Text.class, Text.class, inputPath);


        /* dataset.count(); */
        /* long finishLoadingTime = System.currentTimeMillis(); */
        /* System.out.println("Loading in " + (finishLoadingTime - startTime) / 1000.0 + " seconds"); */
        /* DataSet<TeraKey, TeraValue> dataset = env.readFile(new HadoopTeraInputFormat(), inputPath); */

        /* int parallelism = dataset.getParallelism(); */
        /* env.setParallelism(parallelism); */
        /* System.out.println("debug: parallelism: " + env.getParallelism()); */

        TeraPartitioner teraPartitioner = new TeraPartitioner(parallelism);
        System.out.println("debug: rangeperpart " + teraPartitioner.getRangePerPart() );
        System.out.println("debug: min " + teraPartitioner.getMin() );
        System.out.println("debug: max " + teraPartitioner.getMax() );

        FlinkTeraPartitioner partitioner = new FlinkTeraPartitioner( parallelism );

        DataSet<Tuple2<Text, Text>> result = dataset.partitionCustom(partitioner, 0).sortPartition(0, Order.ASCENDING );
        

        result.count();
        long endComputing = System.currentTimeMillis();
        System.out.println("Loading and Computing in " + (endComputing - startTime)/ 1000.0 + ", seconds");
        //hadoop teraoutputformat
        Job job = Job.getInstance();
        HadoopOutputFormat<Text, Text> hadoopOF = 
            new HadoopOutputFormat<Text, Text>( 
                    new HadoopTeraOutputFormat(), job);
        /* hadoopOF.getConfiguration().set("mapreduce.output.textoutputformat.separator", " "); */
        /* hadoopOF.getConfiguration().set("mapred.output.dir", outputPath); */
        hadoopOF.getConfiguration().set("mapreduce.output.fileoutputformat.outputdir", outputPath);
        hadoopOF.getConfiguration().set("hadoop.dfs.replication", "1");
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        result.output( hadoopOF);

        /* result.output( new TeraTextOutputFormat<Tuple2<Text, Text>>(new Path(outputPath) ) ); */

        /* result.writeAsText(outputPath); */

        /* result.write(new FlinkTeraOutputFormat(), outputPath); */
        /* FileOutputFormat<Text> testFormat = new FileOutputFormat<Text>(); */
        /* result.print(); */

        //test easyarticle's outputformat
        /* JobConf mapredConf = new JobConf(); */
        /* mapredConf.set("fs.defaultFS", "hdfs://master:9000"); */
        /* mapredConf.set("mapreduce.input.fileinputformat.inputdir", inputPath); */
        /* mapredConf.set("mapreduce.output.fileoutputformat.outputdir", outputPath); */
        /* mapredConf.set("mapreduce.job.reduces", "360" ); */
        /* Job jobContext = Job.getInstance(mapredConf); */
        /* HadoopOutputFormat<Text, Text> hadoopOF =  */
        /*     new HadoopOutputFormat<Text, Text>(  */
        /*             new TeraOutputFormat(), jobContext); */
        /* result.output( hadoopOF); */

        env.execute("flink terasort");
    }
}
