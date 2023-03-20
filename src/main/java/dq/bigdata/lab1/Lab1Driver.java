package dq.bigdata.lab1;

import dq.bigdata.lab1.entity.User;
import dq.bigdata.lab1.phase1.SampleMapper;
import dq.bigdata.lab1.phase1.SamplePartitioner;
import dq.bigdata.lab1.phase1.SampleReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class Lab1Driver extends Configured implements Tool {
//    private static final Logger theLogger = Logger.getLogger(Lab1Driver.class);
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf1=new Configuration();

        String[] otherArgs = (new GenericOptionsParser(conf1, strings)).getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: Lab1 <in>: input path");
            System.exit(2);
        }

        conf1.setDouble("Ratio", 0.5);
        Job job1 = Job.getInstance(conf1,"Phase_1");
        job1.setJarByClass(Lab1Driver.class);

        job1.setMapperClass(SampleMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(User.class);

        job1.setPartitionerClass(SamplePartitioner.class);

        job1.setNumReduceTasks(8);
        job1.setReducerClass(SampleReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);

        String phase1OutputFilePath="/lab1/D_Sample";
        FileInputFormat.addInputPath(job1,new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1,new Path(phase1OutputFilePath));

        //ControlledJob controlledJob1=new ControlledJob(job1.getConfiguration());

        return job1.waitForCompletion(true) ? 0 : 1;
    }
    public static void main(String[] args) throws Exception {
        // Make sure there are exactly 2 parameters
        System.out.println(Arrays.toString(args));
        int returnStatus = submitJob(args);
        //theLogger.info("returnStatus="+returnStatus);

        System.exit(returnStatus);
    }
    public static int submitJob(String[] args) throws Exception {
        //String[] args = new String[2];
        //args[0] = inputDir;
        //args[1] = outputDir;
        int returnStatus = ToolRunner.run(new Lab1Driver(), args);
        return returnStatus;
    }

}
