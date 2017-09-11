import org.apache.flink.api.java.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.hadoop.mapreduce.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import terasort.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.lang.Exception;

public class hadoopFormatGrep{
    public static void main(String[] args) throws IOException, Exception{
        if( args.length < 3){
            System.out.println("usage: grep [inputPath] [outputPath] pattern");
            return ;
        }
        String inputPath = args[0];
        String outputPath = args[1];
        final String pattern = args[2];

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //load
        DataSet< Tuple2<Text, Text> > dataset = env.readHadoopFile(
                new HadoopTeraInputFormat(), Text.class, Text.class, inputPath);

        //comput2
        DataSet< Tuple2<Text, Text> > result = dataset.filter( 
            new FilterFunction< Tuple2<Text, Text> >() {
                @Override
                public boolean filter(Tuple2<Text, Text> t){
                    return t.toString().contains(pattern);
                }
            }
        );

        //dumpi
        Job job = Job.getInstance();
        HadoopOutputFormat<Text, Text> hadoopOF = 
            new HadoopOutputFormat<Text, Text>( 
                    new HadoopTeraOutputFormat(), job);
        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        result.output(hadoopOF);

        env.execute("flink grep");
    }
}
