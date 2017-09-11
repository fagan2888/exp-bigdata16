package terasort;

import java.lang.Long;
import java.io.Serializable;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Ints;

import org.apache.hadoop.io.Text;

// class FlinkTeraPartitioner(numPart: Int) extends Partitioner[ Array[ Byte ] ]{
//
//   val min = Longs.fromBytes(0, 0, 0, 0, 0, 0, 0, 0)
//   val max = Longs.fromBytes(0, -1, -1, -1, -1, -1, -1, -1)
//
//   val rangePerPart = (max - min) / numPart + 1
//
//   def partition(key: Array[Byte], numPartitions: Int){
//     val b = key.asInstanceOf[Array[Byte]]
//     val prefix = Longs.fromBytes(0, b(0), b(1), b(2), b(3), b(4), b(5), b(6))
//     // val prefixb = Longs.fromByteArray( new Byte[] { 0, b(0), b(1), b(2), b(3), b(4), b(5), b(6) } )
//     prefix.toInt
//   }
//
// }

public class TeraPartitioner implements Serializable{

  Long min = new Long( 0 ); 
  /* byte[] maxArray = new byte[] {0, -1, -1, -1, -1, -1, -1, -1}; */
  Long max = Longs.fromByteArray( new byte[] {0, -1, -1, -1, -1, -1, -1, -1});
  Long rangePerPart = new Long(1);
  Byte zeroByte = 0;

  public long getRangePerPart(){
      return rangePerPart.longValue();
  }

  public long getMin(){
      return min.longValue();
  }

  public long getMax(){
      return max.longValue();
  }

  public TeraPartitioner( int numPartitions){
      rangePerPart = (max - min) / numPartitions + 1;
  }

  /* long min = ( byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0), byte(0)); */
  /* long max = Longs.fromBytes(0, -1, -1, -1, -1, -1, -1, -1); */
  
  /* long min = Longs.fromBytes( String.getBytes() ); */
  /* long max = Longs.fromBytes(0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff); */

  public int getPartition(Text key){
    /* val b = key.asInstanceOf[Array[Byte]] */
    byte[] b = key.getBytes();
    long prefix = Longs.fromBytes( zeroByte, b[0], b[1], b[2], b[3], b[4], b[5], b[6]);
    // val prefixb = Longs.fromByteArray( new Byte[] { 0, b(0), b(1), b(2), b(3), b(4), b(5), b(6) } )
    /* return 0; */
    return (int) (prefix / rangePerPart);
  }
}
