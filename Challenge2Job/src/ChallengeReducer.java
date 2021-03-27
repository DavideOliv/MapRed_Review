

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChallengeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum=0;
        int count=0;

        for (FloatWritable value: values) {
            sum += value.get();
            count++;

        }


        FloatWritable score = new FloatWritable(sum/count);

        context.write(key, score);


    }


}