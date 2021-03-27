
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChallengeMapper2 extends Mapper<LongWritable, Text, Text, FloatWritable> {

    private HashMap<String, Float> dict = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException{

        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);

        Path[] path = DistributedCache.getLocalCacheFiles(conf);

        parsing(path[0],fs);


    }

    private void parsing(Path path, FileSystem fs) {

        try{
            BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while((line = fis.readLine()) != null) {
                StringTokenizer token = new StringTokenizer(line, " \t");
                this.dict.put(token.nextToken(), Float.parseFloat(token.nextToken()));

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(key.get() == 0 || value.toString().equals("")) return;

        String[] line = value.toString().split("\t");

        if (line.length != 15) return;
        String reviewBody = line[13]; // review body
        String reviewId = line[2];
        String reviewMonth = line[14].split("-")[1];


        StringTokenizer words = new StringTokenizer(reviewBody, " ;!?[](){}\"'-_*&%$:@#.,\\/+=<>");


        while (words.hasMoreTokens()) {
            String word = words.nextToken().toLowerCase();
            if (word.equals("")) continue;
            if (this.dict.containsKey(word)) {
                context.write(new Text(reviewId + "-" + reviewMonth), new FloatWritable(this.dict.get(word)));
            }
            else {
                context.write(new Text(reviewId + "-" + reviewMonth), new FloatWritable(0));
            }
        }



    }

}
