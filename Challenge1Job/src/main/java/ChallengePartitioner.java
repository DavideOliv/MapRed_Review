import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ChallengePartitioner extends Partitioner<Text, FloatWritable> {

    @Override
    public int getPartition(Text key, FloatWritable value, int numReduceTasks) {
        return (Integer.parseInt(key.toString().split("-")[1]));
    }
}