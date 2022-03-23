import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ChallengeDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode =ToolRunner.run(new ChallengeDriver(), args);
        System.exit(exitCode); // se l'output è 0 allora il programma ha terminato correttamente, sennò ci sono stati errori
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Invalid arguments!\n", getClass().getName());
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        String inputDir = args[0]; // path dell'input directory in HDFS
        String outputDir = args[1]; // path dell'output directory in HDFS

        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Job Name: SentiWordNet parser");
        job.setJarByClass(ChallengeDriver.class); // Indico la classe che costituirà l'entry point del job
        job.setMapperClass(ChallengeMapper.class);
        job.setPartitionerClass(ChallengePartitioner.class);
        job.setReducerClass(ChallengeReducer.class);

        job.setNumReduceTasks(13);

        job.setOutputKeyClass(Text.class); // la classe rappresentante il data type dell'output key
        job.setOutputValueClass(FloatWritable.class); // la classe rappresentante il data type dell'output value

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        Path path = new Path("/SentiWordNet_3.txt"); // per debuggare su intellij aggiungere la cartella input

        //System.err.printf(path.toUri().toString() + "\n", getClass().getName());

        job.addCacheFile(path.toUri()); // hdfs://192.168.104.45:9000/sentiwordnet.txt
        //DistributedCache.addCacheFile(path.toUri(), job.getConfiguration());

        boolean success = job.waitForCompletion(true); // success vale true se il job termina correttamente, false altrimenti
        if (!success) {
            throw new IllegalStateException("Job Word Count failed!");
        }
        return 0;
    }


}
