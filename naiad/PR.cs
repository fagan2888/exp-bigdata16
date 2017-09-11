/*
 * Naiad ver. 0.5
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */


//Naiad Big Bug!
//Vertices id have to be consecutive. If you set 1, 1000, then there will be 1000 points!
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using Microsoft.Research.Naiad.Frameworks.Hdfs;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

namespace Microsoft.Research.Naiad.Examples.PR
{
    public class PR : Example
    {
        public string Usage
        {
            get { return "[edgefilenames ...]"; }
        }

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                //// either read inputs from a file, or generate them randomly.
                //Stream<Edge, Epoch> edges;
                //if (args.Length == 1)
                //{
                //    // generate a random graph in each process; pagerank computation is performed on the union of edges.
                //    edges = GenerateEdges(1000000, 20000000, computation.Configuration.ProcessID).AsNaiadStream(computation);
                //}
                //else
                //{
                //    var text = args.Skip(1)
                //                   .AsNaiadStream(computation)
                //                   .Distinct()
                //                   .SelectMany(x => x.ReadLinesOfText());

                //    edges = text.Where(x => !x.StartsWith("#"))
                //                .Select(x => x.Split())
                //                .Select(x => new Edge(new Node(Int32.Parse(x[0])), new Node(Int32.Parse(x[1]))));
                //}

                // loading data
                //Console.WriteLine("Grep program starts");
                if (args.Length < 5)
                {
                    string parameters = "";
                    for (int i = 0; i < args.Length; ++i)
                        parameters = parameters + " " + args[i];
                    Console.WriteLine("current parameters: " + parameters);
                    Console.WriteLine("usage: Examples.exe pr <inputPath> <numIters> <outputPath> <numIterations>");
                    return;
                }
                //for (int i = 0; i < args.Length; ++i){
                //    Console.WriteLine("the "+i+"th argument is " + args[i] );
                //}
                string inputDir = args[1];
                int numFiles = Int32.Parse(args[2]);
                string outputPathFormat = args[3] + "{0}";
                var iterations = Int32.Parse(args[4]);

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                var text = loadDiskFiles(computation, inputDir, numFiles);

                //this will make the whole process slow!
                //var barrierL = text.Select(x => 1);
                //barrierL.Subscribe( list => {
                //    Console.WriteLine("number of edges: " + list.Count());
                //});
                //Console.WriteLine("loading finished at " + stopwatch.Elapsed);

                var edges = text.Select(x => x.Split())
                                .Select(x => new Edge(new Node(Int32.Parse(x[0])), new Node(Int32.Parse(x[1]))));
                Console.Out.WriteLine("Started up!");
                Console.Out.Flush();

                edges = edges.PartitionBy(x => x.source);

                // capture degrees before trimming leaves. countNodes has big problem!
                var degrees = edges.Select(x => x.source)
                                   .CountNodes();


                // initial distribution of ranks.
                var start = degrees.Select(x => x.node.WithValue(0.15f));

                // define an iterative pagerank computation, add initial values, aggregate up the results and print them to the screen.

                //var result = start.NodeJoin(degrees, (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                //        .GraphReduce(edges, (x, y) => x + y, false)
                //        .Select(x => x.node.WithValue((float)(x.value + 0.15)))
                //        .NodeJoin(degrees, (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                //        .GraphReduce(edges, (x, y) => x + y, false);

                var ranks = start.Iterate((lc, deltas) => deltas.PageRankStep(lc.EnterLoop(degrees),
                                                                                           lc.EnterLoop(edges)),
                                                        x => x.node.index,
                                                        iterations,
                                                        "PageRank");


                computation.OnFrontierChange += (x, y) => { Console.WriteLine(stopwatch.Elapsed + "\t" + string.Join(", ", y.NewFrontier)); Console.Out.Flush(); };

                //dumping
                //Action<NodeWithValue<float>, System.IO.BinaryWriter> writer = writeNodeWithValue;
                //rank.WriteToFiles<NodeWithValue<float>>(outputPathFormat, writeNodeWithValue);

                Console.WriteLine("deploy successfully");

                // start computation, and block until completion.
                computation.Activate();
                computation.Join();
            }
        }

        public void writeNodeWithValue(NodeWithValue<float> node, System.IO.BinaryWriter writer)
        {
            string str = node.node.index + "\t" + node.value + "\n";
            writer.Write(Encoding.Default.GetBytes(str), 0, str.Length);
            //writer.Write(str);
        }

        public static IEnumerable<Edge> GenerateEdges(int nodes, int edges, int seed)
        {
            var random = new Random(seed);
            for (int i = 0; i < edges; i++)
                yield return new Edge(new Node(random.Next(nodes)), new Node(random.Next(nodes)));
        }

        public string getFileName(int ith, int totalFiles)
        {
            /* if ith = 2, totalFiles = 11, return 02 */

            //string index = ith.ToString();
            //int len = 1;
            //while (totalFiles / 10 != 0) {
            //    totalFiles /= 10;
            //    len++;
            //}
            //string prefix = "";
            //for (int i = 0; i < len - index.Length; ++i) prefix += "0";

            //return prefix + index;

            /* if ith = 11, return 00011 */
            if (ith < 10) return "0000" + ith.ToString();
            else if (ith < 100) return "000" + ith.ToString();
            else if (ith < 1000) return "00" + ith.ToString();
            else if (ith < 10000) return "0" + ith.ToString();
            else return ith.ToString();
        }

        Stream<string, Epoch> loadDiskFiles(OneOffComputation computation, String inputDir, int numFiles)
        {
            var processes = computation.Configuration.Processes;
            var thisProcess = computation.Configuration.ProcessID;
            List<string> myFiles = new List<string>();

            bool printFirstInput = false;
            for (int i = 0; i < numFiles; i++)
            {
                // ensure we generate the same graph no matter how many processes there are

                if ( (i % processes) == thisProcess)
                {
                    myFiles.Add(inputDir + getFileName(i, numFiles));
                    //Console.Out.WriteLine("my process ID: " + thisProcess.ToString() + ", and my files have " + inputDir + i.ToString());
                    if (!printFirstInput)
                    {
                        Console.Out.WriteLine("my process ID: " + thisProcess.ToString() + ", and my first file is " + inputDir + i.ToString());
                        printFirstInput = true;
                    }
                }
            }
            var text = myFiles.ToArray()
                           .AsNaiadStream(computation)
                           .SelectMany(x => x.ReadLinesOfText());
                           //.Synchronize(x => true);

            //.SelectMany(x => x.ReadLinesOfText());
            return text;
        }

        public string Help
        {
            get { return "Demonstrates pagerank computation using iterative dataflow and GraphLINQ primitives. Repeatedly applies NodeJoin on ranks and degrees to scale values down by degree, followed by GraphReduce to accumulate the ranks along the graph edges."; }
        }
    }


    public static class ExtensionMethods
    {
        // performs one step of pagerank, scaling ranks by (1.0 - reset) / degree, tranmitting to neighbors, aggregating along the way.
        public static Stream<NodeWithValue<float>, T> PageRankStep<T>(this Stream<NodeWithValue<float>, T> ranks,
                                                                           Stream<NodeWithValue<Int64>, T> degrees,
                                                                           Stream<Edge, T> edges)
            where T : Time<T>

        {
            // join ranks with degrees, scaled down. then "graphreduce", accumulating ranks over graph edges.
            return ranks.NodeJoin(degrees, (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                        .GraphReduce(edges, (x, y) => x + y, false)
                        .Select(x => x.node.WithValue((float)(x.value + 0.15)));

        }

        public static IEnumerable<string> ReadLinesOfText(this string filename)
        {
            Console.WriteLine("Reading file {0}", filename);

            if (System.IO.File.Exists(filename))
            {
                //incurs multithread but many system call, which slow the system a lot
                //foreach (var line in System.IO.File.ReadLines(filename).AsParallel().WithDegreeOfParallelism(24))
                //{
                //    yield return line;
                //}

                var file = System.IO.File.OpenText(filename);
                while (!file.EndOfStream)
                    yield return file.ReadLine();

                //var fileLines = System.IO.File.ReadAllLines(filename);
                //foreach(var line in fileLines)
                //    yield return line;
            }
            else
                Console.WriteLine("File not found! {0}", filename);
        }
    }
}
