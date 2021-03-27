import org.apache.hadoop.util.ToolRunner;

public class Challenge{

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ChallengeDriver(), args);
        if(exitCode != 0) {
            System.out.println("NON ESEGUITO IL PRIMO JOB");
            System.exit(exitCode);

        }
        exitCode = ToolRunner.run(new ChallengeDriver2(), args);
        System.exit(exitCode);



    }

}
