import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChallengeDriver2 extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Invalid arguments!\n", getClass().getName());
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }


        String inputDir = args[0]; // path dell'input directory in HDFS
        String outputDir = args[1]; // path dell'output directory in HDFS

        Configuration config2 = new Configuration();
        Job job2 = Job.getInstance(config2, "Job Name : Reviews");
        job2.setJarByClass(Challenge.class);
        job2.setMapperClass(ChallengeMapper2.class);
        //job2.setPartitionerClass((ChallengePartitioner.class));
        job2.setReducerClass(ChallengeReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job2, new Path(inputDir));
        FileOutputFormat.setOutputPath(job2, new Path(outputDir));

        Path pathCache = new Path("/outputDav/senti/part-r-00000");
        DistributedCache.addCacheFile(pathCache.toUri(), job2.getConfiguration());


        boolean success = job2.waitForCompletion(true); // success vale true se il job termina correttamente, false altrimenti


        if (!success) {
            throw new IllegalStateException("Job Word Count failed!");
        }
        return 0;

    }
}


