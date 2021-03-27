

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




public class ChallengeDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.printf("Invalid arguments!\n", getClass().getName());
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }
        //job 1 == Mapreduce SentiWordNet

        //String inputDir = args[0]; // path dell'input directory in HDFS
        //String outputDir = args[1]; // path dell'output directory in HDFS


        Configuration config = new Configuration();
        Job job = Job.getInstance(config, "Job Name: SentiWordNet");
        job.setJarByClass(Challenge.class);
        job.setMapperClass(ChallengeMapper.class);
        job.setReducerClass(ChallengeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path("/SentiWordNet_3.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/outputDav/senti"));


        boolean success = job.waitForCompletion(true); // success vale true se il job termina correttamente, false altrimenti


        if (!success) {
            throw new IllegalStateException("Job Word Count failed!");
        }




        return 0;
    }
}





/*

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
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
import org.apache.hadoop.util.ToolRunner;


public class ChallengeDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ChallengeDriver(), args);
        System.exit(exitCode); // se l'output è 0 allora il programma ha terminato correttamente, sennò ci sono stati errori
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.printf("Invalid arguments!\n", getClass().getName());
            ToolRunner.printGenericCommandUsage(System.err);
            return 1;
        }


        Configuration conf1 = new Configuration();
        Configuration conf2 = new Configuration();

        Job job = new Job(conf1);
        //Job job = Job.getInstance(conf1);
        job.setJarByClass(ChallengeDriver.class);
        job.setMapperClass(ChallengeMapper.class);
        job.setReducerClass(ChallengeReducer.class);

        job.setOutputKeyClass(Text.class); // la classe rappresentante il data type dell'output key
        job.setOutputValueClass(FloatWritable.class); // la classe rappresentante il data type dell'output value

        ControlledJob cjob = new ControlledJob(conf1);
        cjob.setJob(job);
        FileInputFormat.addInputPath(job, new Path("input/SentiWordNet_3.txt"));
        FileOutputFormat.setOutputPath(job, new Path("output/senti"));


        //----------------------------------------------------

        Job job2 = new Job(conf2);
        //Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(ChallengeDriver.class);
        job2.setMapperClass(ChallengeMapper2.class);
        job2.setReducerClass(ChallengeReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        ControlledJob cjob2 = new ControlledJob(conf2);
        cjob2.setJob(job2);
        FileInputFormat.setInputPaths(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));

        JobControl control = new JobControl("control");
        control.addJob(cjob);
        control.addJob(cjob2);
        cjob2.addDependingJob(cjob);

        Thread runControl = new Thread(control);
        runControl.start();
        Path pathCache = new Path("output/senti/part-r-00000");
        DistributedCache.addCacheFile(pathCache.toUri(), job.getConfiguration());





        boolean success = job2.waitForCompletion(true); // success vale true se il job termina correttamente, false altrimenti


        if (!success) {
            throw new IllegalStateException("Job Word Count failed!");
        }
        return 0;


    }
}



 */




