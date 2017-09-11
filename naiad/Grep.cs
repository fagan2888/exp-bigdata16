using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

using Microsoft.Research.Naiad.Frameworks.Lindi;

namespace Microsoft.Research.Naiad.Examples.Grep
{

    public static class ExtensionMethods
    {

        public static IEnumerable<string> ReadLinesOfBinaryBlock(this string filename, int totalLen)
        {
            Console.WriteLine("Reading file {0}", filename);

            //char[] buffer = new char[100];
            if (System.IO.File.Exists(filename))
            {
                byte[] data = System.IO.File.ReadAllBytes(filename);
                for (int i = 0; i < data.Length; i += 100) {
                    //string str = Encoding.UTF8.GetString(data, i, totalLen);
                    string str = Encoding.Default.GetString(data, i, totalLen);
                   
                    yield return str;
                }
                //var file = System.IO.File.OpenText(filename);
                //int index = 0;
                //while (!file.EndOfStream)
                //{
                //    file.ReadBlock(buffer, index, totalLen);

                //    yield return new string(buffer, 0, totalLen);
                //}
            }
            else
                Console.WriteLine("File not found! {0}", filename);
        }
    }

    public class Grep : Example
    {

        public static int KEY_LEN = 10;
        public static int VALUE_LEN = 90;

        public void writeByte(string str, System.IO.BinaryWriter writer) {

            writer.Write(Encoding.Default.GetBytes(str), 0, 100);
        }

        public void Execute(string[] args)
        {
            // the first thing to do is to allocate a computation from args.
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // loading data
                //Console.WriteLine("Grep program starts");
                if (args.Length < 5){
                    string parameters = "";
                    for (int i = 0; i < args.Length; ++i) 
                        parameters = parameters + " " + args[i];
                    Console.WriteLine("current parameters: " + parameters);
                    Console.WriteLine("usage: Examples.exe terasort <inputPath> <numFiles> <pattern> <outputPath>");
                    return ;
                }
                //for (int i = 0; i < args.Length; ++i){
                //    Console.WriteLine("the "+i+"th argument is " + args[i] );
                //}
                string inputDir = args[1];
                int numFiles = Int32.Parse(args[2]);
                string pattern = args[3];
                string outputPathFormat = args[4] + "{0}";

                var text = loadDiskFiles(computation, inputDir, numFiles);
                //text.Subscribe(l =>
                //{
                //    Console.WriteLine("input, # od records: " + l.Count());
                //});

                // computation
                var result = text.Where(x => x.Contains(pattern));

                
                //result.Subscribe(l =>
                //{
                //    Console.WriteLine("result, # of records: " + l.Count());
                //});

                //dumping
                Action<string, System.IO.BinaryWriter> writer = writeByte;
                result.WriteToFiles<string>(outputPathFormat, writer);

                Console.WriteLine("deploy successfully");
                computation.Activate();
                computation.Join();
            }
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
            int totalLen = KEY_LEN + VALUE_LEN;
            var text = myFiles.ToArray()
                           .AsNaiadStream(computation)
                           .SelectMany(x => x.ReadLinesOfBinaryBlock(totalLen));
                           //.Synchronize(x => true)
         
            return text;
        }

        public string Usage { get { return ""; } }

        public string Help { get { return "Demonstration of Grep on Naiad"; } }
    }
}
