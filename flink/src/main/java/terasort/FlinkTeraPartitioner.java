package terasort;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.hadoop.io.Text;

public class FlinkTeraPartitioner implements Partitioner<Text>{

    /* TeraPartitioner partitioner = new TeraPartitioner(1); */
    /* public FlinkTeraPartitioner( TeraPartitioner partitioner){ */
    /*     this.partitioner = new TeraPartitioner(partitioner); */
    /* } */

    TeraPartitioner partitioner = null; 
    public FlinkTeraPartitioner( int parallelism){
        partitioner = new TeraPartitioner(parallelism);
    }

    @Override
        public int partition(Text key, int numPartitions){

            return partitioner.getPartition(key);
        }
}
