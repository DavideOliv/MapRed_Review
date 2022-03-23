import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChallengeMapper extends Mapper<LongWritable, Text, Text, FloatWritable> { // i primi 2 data types si riferiscono alla coppia key-value in input, gli altri 2 alla coppia in output

    private final HashMap<String, Float> dict = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException{
    /*
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.getLocal(conf);

        Path[] dataFile = DistributedCache.getLocalCacheFiles(conf);
        parseSentiWord(dataFile[0], fs);

     */

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                parseSentiWord(path, fs);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void parseSentiWord(Path patternsFileName, FileSystem fs) {
        try {
            BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(patternsFileName)));
            String line;
            HashMap<String, Float[]> dict = new HashMap<>();
            while ((line = fis.readLine()) != null) {
                if (line.startsWith("#") || line.startsWith("\t")) {
                    continue;
                }
                String[] text=line.split("\t"); //String casting and tsv splitting. We need 2, 3, 4 starting at 0
                float plus = (text[2].equals("")) ? 0f : Float.parseFloat(text[2]);
                float minus = (text[3].equals("")) ? 0f : Float.parseFloat(text[3]);
                float wordsScore = plus - minus; // PosScore - NegScore
                //float wordsScore = Float.parseFloat(text[2]) - Float.parseFloat(text[3]); // PosScore - NegScore
                String[] synsets = text[4].split(" ");
                for(String synset: synsets) {
                    String keyout = synset.split("#")[0]; // able#1 -> able 1 -> able
                    if (dict.containsKey(keyout)) {
                        Float[] temp = dict.get(keyout);
                        dict.put(keyout, new Float[] {temp[0] + wordsScore, temp[1]+1});  // TODO controllare se funziona cambiando direttamente temp
                    } else {
                        dict.put(keyout, new Float[]{wordsScore, 1f});
                    }
                }
            }

            for (String key : dict.keySet()) {
                Float[] temp = dict.get(key);
                this.dict.put(key.toLowerCase(), temp[0]/temp[1]); // TODO vedere se serve il to lower case
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() == 0 || value.toString().equals("")) return;
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

        //System.err.printf(reviewBody + " IN : " + Integer.toString(in) + " OUT : " + Integer.toString(out) +"\n", getClass().getName());
    }

}
