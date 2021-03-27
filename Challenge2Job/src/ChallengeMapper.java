

import java.io.IOException;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChallengeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(value.toString().startsWith("#") || value.toString().startsWith("\t") || value.toString().isEmpty())  return;

        String[] line = value.toString().split("\t");
        float pos = (line[2].equals("")) ? 0f : Float.parseFloat(line[2]);
        float neg = (line[3].equals("")) ? 0f : Float.parseFloat(line[3]);


        float score = pos - neg;

        String[] synsets = line[4].split(" ");

        for(String synset : synsets){
            String keyout = synset.split("#")[0];
            context.write(new Text(keyout), new FloatWritable(score));

        }




    }

}