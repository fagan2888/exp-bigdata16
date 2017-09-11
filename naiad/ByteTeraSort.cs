using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;


namespace Microsoft.Research.Naiad.Examples.ByteTeraSort
{

    public static class ExtensionMethods
    {
        public class ByteArrayComparer : IComparer<Pair<byte[], byte[]>>
        {
            public int Compare(Pair< byte[], byte[]>  x, Pair< byte[], byte[]> y)
            {
                // implement your comparison criteria here
                for (int i = 0; i < 10; ++i) {
                    if (x.First[i] < y.First[i]) return -1;
                    else if (x.First[i] > y.First[i]) return 1;
                }
                    return  0;

            }
        }

        public static IEnumerable<Pair<Byte[], Byte[]>> ReadLinesOfKeyValue(this string filename, int keyLen, int valueLen)
        {
            Console.WriteLine("Reading file {0}", filename);

            int totalLen = keyLen + valueLen;
            char[] buffer = new char[totalLen];
            if (System.IO.File.Exists(filename))
            {
                var allBytes = System.IO.File.ReadAllBytes(filename);

                int index = 0;
                while(index < allBytes.Length){
                    byte[] key = new byte[keyLen];
                    byte[] value = new byte[valueLen];
                    for (int j = 0; j < keyLen; ++j)
                    {
                        key[j] = allBytes[index++];
                    }
                    for (int j = 0; j < valueLen; ++j)
                    {
                        value[j] = allBytes[index++];
                    }
                    yield return new Pair<byte[], byte[]>(key, value);
                }
                
                //var file = System.IO.File.OpenText(filename);
                //int index = 0;
                //while (!file.EndOfStream)
                //{
                //    file.ReadBlock(buffer, index, totalLen);

                //    //yield return new Pair<string, string>(new string(buffer, 0, keyLen), new string(buffer, keyLen, valueLen));
                //}
            }
            else
                Console.WriteLine("File not found! {0}", filename);
        }

        /// <summary>
        /// Counts records in the input stream, emitting new counts as they change.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>stream of counts</returns>
        public static Stream<Pair<byte[], byte[]>, Epoch> TeraSort(this Stream<Pair<byte[], byte[]>, Epoch> stream)
        {
            //return stream.NewUnaryStage((i, s) => new SortVertex(i, s), x => partitioner.getPartition( x.First ) , null, "TeraSort");
            return stream.NewUnaryStage((i, s) => new SortVertex(i, s), null, null, "TeraSort");
        }

        /// <summary>
        /// A Naiad vertex for sorting records of type S. Each epoch, sorted record are produced as output.
        /// </summary>
        internal class SortVertex : UnaryVertex<Pair<byte[], byte[]>, Pair<byte[], byte[]>, Epoch>
        {
            // we maintain dataset.
            List<Pair<byte[], byte[]>> dataset = new List<Pair<byte[], byte[]>>();

            // Each batch of records of type TRecord we receive, we must add them in list.
            public override void OnReceive(Message<Pair<byte[], byte[]>, Epoch> message)
            {
                this.NotifyAt(message.time);
                // a message contains length valid records.
                for (int i = 0; i < message.length; i++)
                {
                    var data = message.payload[i];
                    dataset.Add(data);
                }
            }

            // once all records of an epoch are received, we should send the result along.
            public override void OnNotify(Epoch time)
            {
                var output = this.Output.GetBufferForTime(time);
                //dataset.Sort((x, y) => x.First.ToArray().CompareTo(y.First.ToArray()));
                dataset.Sort(new ByteArrayComparer() );
                foreach (var element in dataset)
                {
                    output.Send(element);
                }
            }

            // the UnaryVertex base class needs to know the index and stage of the vertex. 
            public SortVertex(int index, Stage<Epoch> stage) : base(index, stage) { }
        }
    }

    public class TeraSortPartitioner
    {

        public long min = BitConverter.ToInt64(new Byte[] { 0, 0, 0, 0, 0, 0, 0, 0 }, 0);
        public long max = BitConverter.ToInt64(new Byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00 }, 0);
        public long rangePerPart = 1;
        public TeraSortPartitioner(int numPartitions)
        {
            rangePerPart = (max - min) / numPartitions + 1;
        }
        public int getPartition(byte[] key)
        {
            //Byte[] keyBytes = System.Text.Encoding.ASCII.GetBytes(key.Substring(0, 8));

            //if (BitConverter.IsLittleEndian)
            //    Array.Reverse(keyBytes);
            ////BitConverter transforms from the right side
            //keyBytes[7] = 0;
            long keyLongValue = 0;
            long index = 1;
            for (int i = 6; i >= 0; --i)
            {

                keyLongValue += index * (int)key[i];
                index *= 256;
            }

            return (int)(keyLongValue / rangePerPart);
        }
    }

    public class ByteTeraSort : Example
    {

        public static int KEY_LEN = 10;
        public static int VALUE_LEN = 90;

        public void Execute(string[] args)
        {
            // the first thing to do is to allocate a computation from args.
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // loading data
                if (args.Length < 5)
                {
                    Console.WriteLine("usage: Examples.exe terasort <inputDir> <numFiles> <outputDir> <numWorkers=6>");
                }
                String inputDir = args[1];
                int numFiles = Int32.Parse(args[2]);
                string outputPathFormat = args[3] + "{0}";
                int numWorkers = Int32.Parse(args[4]);

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                var text = loadDiskFiles(computation, inputDir, numFiles);
                //text.Subscribe(l => {
                //    foreach (var v in l) Console.WriteLine("key is " + v.First + " , value is " + v.Second);
                //});

                // computation
                TeraSortPartitioner partitioner = new TeraSortPartitioner(computation.Configuration.Processes * numWorkers);
                //var result = text.TeraSort(partitioner);

                var result = text.PartitionBy(x => partitioner.getPartition(x.First)).TeraSort();


                //Console.WriteLine("max: " + partitioner.max);
                //Console.WriteLine("min: " + partitioner.min);
                //Console.WriteLine("rangePart: " + partitioner.rangePerPart);

                //char[] array = new char[4];
                //array[0] = 'h';
                //array[1] = 'e';
                //array[2] = 'l';
                //array[3] = 'o';

                //string testString = new string(array, 0, 3);
                //Console.WriteLine("testString: " + testString);
                //Console.WriteLine("2n pos: " + (int)testString[1]);

                //var result = text.PartitionBy(x => x.First.GetHashCode() ).TeraSort();


                //long keyLongValue = 0;
                //long index = 1;
                //for (int i = 6; i >= 0; --i)
                //{
                //    keyLongValue += index * 255;
                //    index *= 256;
                //}
                //Console.WriteLine("max, calculated: " + keyLongValue);

                // string testStr = "00000000";
                // Byte[] keyBytes = System.Text.Encoding.Default.GetBytes(testStr.Substring(0, 8));
                //// for (int i = 0; i < 7; ++i) keyBytes[i] = 0;
                // keyBytes[7] = 0;
                // Array.Reverse(keyBytes);
                // long keyLongValue = BitConverter.ToInt64(keyBytes, 0);
                // Console.WriteLine("keyLongValue: " + keyLongValue);
                // if (keyLongValue < 0) Console.WriteLine("Wrong !!!!!");



                //result.Subscribe(l =>
                //{
                //    foreach (var v in l) Console.WriteLine("key is " + v.First + " , value is " + v.Second);
                //});
                computation.OnFrontierChange += (x, y) => { Console.WriteLine(stopwatch.Elapsed + "\t" + string.Join(", ", y.NewFrontier)); Console.Out.Flush(); };
                //long max = BitConverter.ToInt64(new Byte[] { 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00 }, 0);
                //Console.WriteLine("long max equal to " + max);

                //dumping
               // Action< Pair<byte[], byte[]> , System.IO.BinaryWriter > writer = writePair;
                //result.WriteToFiles<Pair<byte[], byte[]>>(outputPathFormat, writer);
                computation.Activate();
                computation.Join();
            }
        }

        public void writePair(Pair<byte[], byte[]> node, System.IO.BinaryWriter writer)
        {
            //string str = "" + node.First + node.Second;
            //writer.Write(Encoding.ASCII.GetBytes(str), 0, str.Length);
            writer.Write(node.First, 0, 10);
            writer.Write(node.Second, 0, 90);
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

        Stream<Pair<Byte[], Byte[]>, Epoch> loadDiskFiles(OneOffComputation computation, String inputDir, int numFiles)
        {
            var processes = computation.Configuration.Processes;
            var thisProcess = computation.Configuration.ProcessID;
            List<string> myFiles = new List<string>();
            for (int i = 0; i < numFiles; i++)
            {
                // ensure we generate the same graph no matter how many processes there are

                if ((i % processes) == thisProcess)
                {
                    myFiles.Add(inputDir + getFileName(i, numFiles));
                    Console.Out.WriteLine("my process ID: " + thisProcess.ToString() + ", and my files have " + inputDir + i.ToString());
                }
            }
            var text = myFiles.ToArray()
                           .AsNaiadStream(computation)
                           .Synchronize(x => true)
                           .SelectMany(x => x.ReadLinesOfKeyValue(KEY_LEN, VALUE_LEN));
            //.SelectMany(x => x.ReadLinesOfText());
            return text;
        }

        public string Usage { get { return ""; } }

        public string Help { get { return "Demonstration of Terasrot on Naiad"; } }
    }
}
