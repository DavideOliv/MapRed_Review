import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChallengeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException { //i primi 2 parametri sono la coppia key-(list of values) )
        float sum = 0;
        int size = 0;
        for (FloatWritable value: values) {
            sum += value.get();
            size++;
        }
        float mean = Math.round((sum / size) * 100f) / 100f;
        if (mean > 0) {
            context.write(new Text(key.toString().split("-")[0]), new FloatWritable(mean));
        }

    }
}