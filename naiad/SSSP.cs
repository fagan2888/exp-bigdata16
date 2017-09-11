﻿/*
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using System.Diagnostics;

namespace Microsoft.Research.Naiad.Examples.SSSP
{
    public static class ExtensionMethods
    {

        // takes a graph as an edge stream and produces a stream of pairs (x,y) where y is the least vertex capable of reaching vertex x.
        public static Stream<Pair<TVertex, float>, Epoch> SSSP<TVertex>(this Stream<Pair<TVertex, TVertex>, Epoch> edges, TVertex srcId)
            where TVertex : IEquatable<TVertex>, IComparable<TVertex>
        {
            // prepartitioning reduces exchanges by one.
            edges = edges.PartitionBy(x => x.First.GetHashCode());

            // initial labels are (node, node).
            //var labels = edges.Select(x => x.First)
            //                  .Distinct()
            //                  .Select(x =>
            //                  {
            //                      if (x.CompareTo(srcId) == 0) return new Pair<TVertex, float>(x, (float)0);
            //                      else return new Pair<TVertex, float>(x, float.MaxValue);
            //                  }).PartitionBy(x => x.First.GetHashCode());

            var labels = edges.Where(x => (x.First.CompareTo(srcId) == 0)).Select(x => x.First)
                              .Distinct()
                              .Select(x => new Pair<TVertex, float>(x, (float)0))
                              .PartitionBy(x => x.First.GetHashCode());
           // return labels;

            // repeatedly exchange labels with neighbors, keeping the least observed labels.
            // emits all exchanged labels; a BlockingAggregate reduces this to one per node.
            // can't use, this is faster but incorrect! return labels.Iterate((lc, z) => z.GraphJoin(lc.EnterLoop(edges))

            return labels.IterateAndAccumulate((lc, z) => z.GraphJoin(lc.EnterLoop(edges))
                                                           .Select(pair =>
                                                           {
                                                               float newValue = pair.Second + 1;
                                                               // if (newValue != float.MaxValue) newValue = newValue + 1;
                                                               if (pair.First.CompareTo(srcId) == 0) newValue = 0;
                                                               return new Pair<TVertex, float>(pair.First, newValue);
                                                           })
                                                           .BlockingAggregate((x, y) => x.CompareTo(y) < 0 ? x : y)
                //.Synchronize()  // optionally sync up everything each iteration
                                                           ,
                                               x => x.First.GetHashCode(), Int32.MaxValue, "Iteration")
                         //.Concat(labels)
                         .BlockingAggregate((x, y) => x.CompareTo(y) < 0 ? x : y);
        }

        #region Streaming aggregation

        /// <summary>
        /// Aggregates key-value pairs, producing new outputs whenever an aggregate changes.
        /// </summary>
        /// <typeparam name="TKey">key type</typeparam>
        /// <typeparam name="TValue">value type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="input">input key-value stream</param>
        /// <param name="aggregate">aggregation function</param>
        /// <returns>aggregated key-value pairs</returns>
        public static Stream<Pair<TKey, TValue>, TTime> StreamingAggregate<TKey, TValue, TTime>(this Stream<Pair<TKey, TValue>, TTime> input, Func<TValue, TValue, TValue> aggregate)
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            return Foundry.NewUnaryStage(input, (i, s) => new StreamingAggregateVertex<TKey, TValue, TTime>(i, s, aggregate), x => x.First.GetHashCode(), x => x.First.GetHashCode(), "StreamingAggregate");
        }

        /// <summary>
        /// Aggregates key-value pairs, and produces new output whenever the result changes. 
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        public class StreamingAggregateVertex<TKey, TValue, TTime> : UnaryVertex<Pair<TKey, TValue>, Pair<TKey, TValue>, TTime>
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            private readonly Dictionary<TKey, TValue> Values;
            private readonly Func<TValue, TValue, TValue> Aggregate;

            public override void OnReceive(Message<Pair<TKey, TValue>, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int j = 0; j < message.length; j++)
                {
                    var record = message.payload[j];

                    // if the key is new, install the new value.
                    if (!this.Values.ContainsKey(record.First))
                    {
                        this.Values[record.First] = record.Second;
                        output.Send(record);
                    }
                    // else update value and send if it changes.
                    else
                    {
                        var oldValue = this.Values[record.First];
                        var newValue = this.Aggregate(oldValue, record.Second);
                        if (!oldValue.Equals(newValue))
                        {
                            this.Values[record.First] = newValue;
                            output.Send(new Pair<TKey, TValue>(record.First, newValue));
                        }
                    }
                }
            }

            public StreamingAggregateVertex(int index, Stage<TTime> vertex, Func<TValue, TValue, TValue> aggregate)
                : base(index, vertex)
            {
                this.Values = new Dictionary<TKey, TValue>();
                this.Aggregate = aggregate;

                this.Entrancy = 1;
            }
        }

        #endregion


        public static Stream<Pair<TKey, TValue>, TTime> LocalBlockingAggregate<TKey, TValue, TTime>(this Stream<Pair<TKey, TValue>, TTime> input, Func<TValue, TValue, TValue> aggregate)
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            return Foundry.NewUnaryStage(input, (i, s) => new BlockingAggregateVertex<TKey, TValue, TTime>(i, s, aggregate), null, x => x.First.GetHashCode(), "BlockingAggregate");
        }


        #region Blocking aggregation

        /// <summary>
        /// Aggregates key-value pairs, producing at most one output per key per time.
        /// </summary>
        /// <typeparam name="TKey">key type</typeparam>
        /// <typeparam name="TValue">value type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="input">input key-value stream</param>
        /// <param name="aggregate">aggregation function</param>
        /// <returns>aggregated key-value pairs</returns>
        public static Stream<Pair<TKey, TValue>, TTime> BlockingAggregate<TKey, TValue, TTime>(this Stream<Pair<TKey, TValue>, TTime> input, Func<TValue, TValue, TValue> aggregate)
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            return Foundry.NewUnaryStage(input, (i, s) => new BlockingAggregateVertex<TKey, TValue, TTime>(i, s, aggregate), x => x.First.GetHashCode(), x => x.First.GetHashCode(), "BlockingAggregate");
        }

        /// <summary>
        /// Aggregates key-value pairs, and produces one new output whenever for each time in which a value changes.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        public class BlockingAggregateVertex<TKey, TValue, TTime> : UnaryVertex<Pair<TKey, TValue>, Pair<TKey, TValue>, TTime>
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            private readonly HashSet<TKey> Active = new HashSet<TKey>();
            private readonly Dictionary<TKey, TValue> Values = new Dictionary<TKey, TValue>();
            private readonly Func<TValue, TValue, TValue> Aggregate;

            public override void OnReceive(Message<Pair<TKey, TValue>, TTime> message)
            {
                for (int j = 0; j < message.length; j++)
                {
                    var record = message.payload[j];
                    var time = message.time;

                    // if the key is new, install the value.
                    if (!this.Values.ContainsKey(record.First))
                    {
                        this.Values[record.First] = record.Second;
                        this.Active.Add(record.First);
                        this.NotifyAt(time);
                    }
                    // else update value and send if it changes
                    else
                    {
                        var oldValue = this.Values[record.First];
                        var newValue = this.Aggregate(oldValue, record.Second);
                        if (!oldValue.Equals(newValue))
                        {
                            this.Values[record.First] = newValue;
                            this.Active.Add(record.First);
                            this.NotifyAt(time);
                        }
                    }
                }
            }

            public override void OnNotify(TTime time)
            {
                var output = this.Output.GetBufferForTime(time);
                foreach (var key in this.Active)
                    output.Send(key.PairWith(this.Values[key]));

                this.Active.Clear();
            }

            public BlockingAggregateVertex(int index, Stage<TTime> vertex, Func<TValue, TValue, TValue> aggregate)
                : base(index, vertex)
            {
                this.Aggregate = aggregate;
            }
        }

        #endregion

        #region Graph-based Join

        public static Stream<Pair<TVertex, TState>, TTime> GraphJoin<TVertex, TState, TTime>(this Stream<Pair<TVertex, TState>, TTime> values, Stream<Pair<TVertex, TVertex>, TTime> edges)
            where TTime : Time<TTime>
        {
            return Foundry.NewBinaryStage(edges, values, (i, s) => new GraphJoinVertex<TVertex, TState, TTime>(i, s), x => x.First.GetHashCode(), y => y.First.GetHashCode(), null, "GraphJoin");
        }

        public class GraphJoinVertex<TVertex, TState, TTime> : BinaryVertex<Pair<TVertex, TVertex>, Pair<TVertex, TState>, Pair<TVertex, TState>, TTime>
            where TTime : Time<TTime>
        {
            private readonly Dictionary<TVertex, List<TVertex>> edges = new Dictionary<TVertex, List<TVertex>>();
            private readonly List<Pair<TVertex, TState>> enqueued = new List<Pair<TVertex, TState>>();
            private bool graphReceived = false;

            //Construct adjacent lists, stored on edges !
            public override void OnReceive1(Message<Pair<TVertex, TVertex>, TTime> message)
            {
                this.NotifyAt(message.time);

                for (int i = 0; i < message.length; i++)
                {
                    var edge = message.payload[i];

                    if (!this.edges.ContainsKey(edge.First))
                        this.edges.Add(edge.First, new List<TVertex>());

                    this.edges[edge.First].Add(edge.Second); // adjacent list
                }
            }

            public override void OnReceive2(Message<Pair<TVertex, TState>, TTime> message)
            {
                if (!this.graphReceived)
                {
                    // add each record to a queue of work todo.
                    for (int i = 0; i < message.length; i++)
                        this.enqueued.Add(message.payload[i]);
                }
                else
                {
                    var output = this.Output.GetBufferForTime(message.time);

                    for (int i = 0; i < message.length; i++)
                    {
                        var data = message.payload[i];

                        // send data to any matching neighbors.
                        if (this.edges.ContainsKey(data.First))
                            foreach (var destination in this.edges[data.First])
                                output.Send(destination.PairWith(data.Second));
                    }
                }
            }

            public override void OnNotify(TTime time)
            {
                // we've received the entire graph (put this at the end of the method to see re-entrancy at work / crashing!)
                this.graphReceived = true;

                var output = this.Output.GetBufferForTime(time);

                // send matches with enqueued data.
                foreach (var data in this.enqueued)
                    if (this.edges.ContainsKey(data.First))
                        foreach (var destination in this.edges[data.First])
                            output.Send(destination.PairWith(data.Second));

                this.enqueued.Clear();
            }

            public GraphJoinVertex(int index, Stage<TTime> vertex)
                : base(index, vertex)
            {
                this.Entrancy = 5;
            }
        }

        #endregion

        public static IEnumerable<string> ReadLinesOfText(this string filename)
        {
            Console.WriteLine("Reading file {0}", filename);

            if (System.IO.File.Exists(filename))
            {
                var file = System.IO.File.OpenText(filename);
                while (!file.EndOfStream)
                    yield return file.ReadLine();
            }
            else
                Console.WriteLine("File not found! {0}", filename);
        }
    }

    public class SSSP : Example
    {
        public string Usage
        {
            get { return "[nodes edges]"; }
        }

        public void Execute(string[] args)
        {
            // allocate a new computation from command line arguments.
            using (var computation = NewComputation.FromArgs(ref args))
            {
                //var nodeCount = args.Length == 3 ? Convert.ToInt32(args[1]) : 1000;
                //var edgeCount = args.Length == 3 ? Convert.ToInt32(args[2]) : 2000;

                //#region Generate a local fraction of input data

                //var random = new Random(0);
                //var processes = computation.Configuration.Processes;
                //var thisProcess = computation.Configuration.ProcessID;
                //var graphFragmentList = new List<Pair<int, int>>();
                //for (int i = 0; i < edgeCount; i++)
                //{
                //    // ensure we generate the same graph no matter how many processes there are
                //    var edge = new Pair<int, int>(random.Next(nodeCount), random.Next(nodeCount));
                //    if ((i % processes) == thisProcess)
                //    {
                //        graphFragmentList.Add(edge);
                //    }
                //}
                //var graphFragment = graphFragmentList.ToArray();

                //#endregion

                //Console.WriteLine("size of graphFragmentList: {0}", graphFragmentList.Count());
                //Console.WriteLine("Computing components of a random graph on {0} nodes and {1} edges", nodeCount, edgeCount);

                Stopwatch stopwatch = new Stopwatch();

                //// convert array of edges to single-epoch stream.
                //var edges = graphFragment.AsNaiadStream(computation)
                //                         .Synchronize(x => true);


                if (args.Length < 5)
                {
                    string parameters = "";
                    for (int i = 0; i < args.Length; ++i)
                        parameters = parameters + " " + args[i];
                    Console.WriteLine("current parameters: " + parameters);
                    Console.WriteLine("usage: Examples.exe sssp <inputPath> <numFiles> <outputPath> <srcId>");
                    return;
                }
                //for (int i = 0; i < args.Length; ++i){
                //    Console.WriteLine("the "+i+"th argument is " + args[i] );
                //}
                string inputDir = args[1];
                int numFiles = Int32.Parse(args[2]);
                string outputPathFormat = args[3] + "{0}";
                var srcId = Int32.Parse(args[4]);

                //loading
                var text = loadDiskFiles(computation, inputDir, numFiles);

                //building graph
                var edges = text.Select(x => x.Split())
                                .Select(x => new Pair<int, int>(Int32.Parse(x[0]), Int32.Parse(x[1])));

                var result = edges.SSSP(srcId);
                     //.Subscribe(list => {
                     //    Console.WriteLine("labeled {0} nodes in {1}", list.Count(), stopwatch.Elapsed);
                     //    foreach (var element in list) {
                     //        Console.WriteLine("vertex: " + element.First + ",value: " + element.Second);
                     //    }
                     //});

                //edges.Subscribe(list =>
                //{
                //    int numEdges = 0;
                //    foreach (var element in list)
                //    {
                //        numEdges++;
                //    }
                //    Console.WriteLine("# of edges is : {0}", numEdges);
                //});

                //dumping
                Action<Pair<int, float>, System.IO.BinaryWriter> writer = writePair;
                result.WriteToFiles<Pair<int, float>>(outputPathFormat, writer);

                //Console.WriteLine("deploy successfully");
                
                stopwatch.Start();
                computation.OnFrontierChange += (x, y) => { Console.WriteLine(stopwatch.Elapsed + "\t" + string.Join(", ", y.NewFrontier)); Console.Out.Flush(); };
                computation.Activate();     // start graph computation
                computation.Join();         // block until computation completes
            }

        }

        public void writePair<VType1, VType2>(Pair<VType1, VType2> node, System.IO.BinaryWriter writer)
        {
            string str = node.First + "\t" + node.Second + "\n";
            writer.Write(Encoding.Default.GetBytes(str), 0, str.Length);
            //writer.Write(str);
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

                if ((i % processes) == thisProcess)
                {
                    myFiles.Add(inputDir + getFileName(i, numFiles));
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
            //.SelectMany(x => x.ReadLinesOfText());
            return text;
        }

        public string Help
        {
            get { return "Demonstrates an iterative dataflow computation, using streaming aggregation within the loop and a blocking aggregation outside the loop. Demonstrates how optional coordination can give good performance when not used and determinism when used."; }
        }
    }
}
