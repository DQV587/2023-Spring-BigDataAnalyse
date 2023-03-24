package dq.bigdata.lab1;

import dq.bigdata.lab1.entity.User;
import dq.bigdata.lab1.phase1.SampleMapper;
import dq.bigdata.lab1.phase3.StandardizeMapper;
import dq.bigdata.lab1.phase3.StandardizeReducer;
import dq.bigdata.lab1.phase4.FillMapper;
import dq.bigdata.lab1.phase4.FillReducer;
import dq.bigdata.lab1.phase4_pre.AvgMapper;
import dq.bigdata.lab1.phase4_pre.AvgReducer;
import dq.bigdata.lab1.utils.CareerPartitioner;
import dq.bigdata.lab1.phase1.SampleReducer;
import dq.bigdata.lab1.phase2.FilterMapper;
import dq.bigdata.lab1.phase2.FilterReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;

public class Lab1Driver extends Configured implements Tool {
//    private static final Logger theLogger = Logger.getLogger(Lab1Driver.class);
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf1=new Configuration();

        String[] otherArgs = (new GenericOptionsParser(conf1, strings)).getRemainingArgs();
        if (otherArgs.length != 5) {
            System.err.println("Usage: Lab1 <in>: input path");
            System.exit(2);
        }

        conf1.setDouble("Ratio", 0.5);
        Job job1 = Job.getInstance(conf1,"Phase_1");
        job1.setJarByClass(Lab1Driver.class);

        job1.setMapperClass(SampleMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(User.class);

        job1.setPartitionerClass(CareerPartitioner.class);

        job1.setNumReduceTasks(8);
        job1.setReducerClass(SampleReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);

        String inputFilePath=otherArgs[0];
        String phase1OutputFilePath=otherArgs[1];

        FileInputFormat.addInputPath(job1,new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job1,new Path(phase1OutputFilePath));

        ControlledJob controlledJob1=new ControlledJob(job1.getConfiguration());

        Configuration conf2=new Configuration();
        conf2.setDouble("LongitudeMax",11.1993265);
        conf2.setDouble("LongitudeMin",8.1461259);
        conf2.setDouble("LatitudeMax",57.750511);
        conf2.setDouble("LatitudeMin",56.5824856);


        Job job2=Job.getInstance(conf2,"Phase_2");
        job2.setJarByClass(Lab1Driver.class);

        job2.setMapperClass(FilterMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(User.class);
        job2.setReducerClass(FilterReducer.class);
        job2.setOutputKeyClass(User.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setNumReduceTasks(8);
        job2.setPartitionerClass(CareerPartitioner.class);

        String phase2OutputFilePath=otherArgs[2];
        FileInputFormat.addInputPath(job2,new Path(phase1OutputFilePath));
        FileOutputFormat.setOutputPath(job2,new Path(phase2OutputFilePath));

        ControlledJob controlledJob2=new ControlledJob(job2.getConfiguration());
        controlledJob2.addDependingJob(controlledJob1);
//
//        Configuration conf3_pre=new Configuration();
//        Job job3_pre=Job.getInstance(conf3_pre,"Phase_3_Pre");
//        job3_pre.setJarByClass(Lab1Driver.class);
//
//        job3_pre.setMapperClass(FindMaxMinMapper.class);
//        job3_pre.setReducerClass(FindMaxMinReducer.class);
//        job3_pre.setNumReduceTasks(1);
//
//        String phase3_PreOutputFilePath="/lab1/MaxMin";
//        FileInputFormat.addInputPath(job3_pre,new Path(phase2OutputFilePath));
//        FileOutputFormat.setOutputPath(job3_pre,new Path(phase3_PreOutputFilePath));
//
//        ControlledJob controlledJob3_pre=new ControlledJob(job3_pre.getConfiguration());
//        controlledJob3_pre.addDependingJob(controlledJob2);


        Configuration conf3=new Configuration();
        conf3.setDouble("RatingMax",102.71);
        conf3.setDouble("RatingMin",-261.13);

        Job job3=Job.getInstance(conf3,"Phase_3");
        job3.setJarByClass(Lab1Driver.class);

        job3.setMapperClass(StandardizeMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(User.class);
        job3.setNumReduceTasks(8);
        job3.setPartitionerClass(CareerPartitioner.class);
        job3.setReducerClass(StandardizeReducer.class);
        job3.setOutputKeyClass(User.class);
        job3.setOutputValueClass(NullWritable.class);

        String phase3OutputFilePath=otherArgs[3];
        FileInputFormat.addInputPath(job3,new Path(phase2OutputFilePath));
        FileOutputFormat.setOutputPath(job3,new Path(phase3OutputFilePath));

        ControlledJob controlledJob3=new ControlledJob(job3.getConfiguration());
        controlledJob3.addDependingJob(controlledJob2);

//        Configuration conf4_pre1=new Configuration();
//        Job job4_pre1=Job.getInstance(conf4_pre1,"Phase4_Pre");
//        job4_pre1.setJarByClass(Lab1Driver.class);
//        job4_pre1.setMapperClass(AvgMapper.class);
//        job4_pre1.setReducerClass(AvgReducer.class);
//        job4_pre1.setOutputKeyClass(Text.class);
//        job4_pre1.setOutputValueClass(DoubleWritable.class);
//        FileInputFormat.addInputPath(job4_pre1,new Path(phase3OutputFilePath));
//        FileOutputFormat.setOutputPath(job4_pre1,new Path("/lab1/phase4/avgRating"));
//
//        ControlledJob controlledJob4Pre=new ControlledJob(job4_pre1.getConfiguration());
//        controlledJob4Pre.addDependingJob(controlledJob3);

        Configuration conf4=new Configuration();
        Job job4=Job.getInstance(conf4,"Phase4");
        job4.setJarByClass(Lab1Driver.class);
        job4.setMapperClass(FillMapper.class);
        job4.setReducerClass(FillReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(User.class);
        job4.setOutputKeyClass(User.class);
        job4.setOutputValueClass(NullWritable.class);
        job4.setPartitionerClass(CareerPartitioner.class);
        job4.setNumReduceTasks(8);

        String phase4OutPutFilePath=otherArgs[4];
        FileInputFormat.addInputPath(job4,new Path(phase3OutputFilePath));
        FileOutputFormat.setOutputPath(job4,new Path(phase4OutPutFilePath));

        ControlledJob controlledJob4=new ControlledJob(job4.getConfiguration());
        controlledJob4.addDependingJob(controlledJob3);

        JobControl jc=new JobControl("lab1");
        jc.addJob(controlledJob1);
        jc.addJob(controlledJob2);
//        jc.addJob(controlledJob3_pre);
        jc.addJob(controlledJob3);
//        jc.addJob(controlledJob4Pre);
        jc.addJob(controlledJob4);
        Thread jcThread = new Thread(jc);
        jcThread.start();
        while(true){
            //当job池里所有的job完成后,执行 下一步操作
            if(jc.allFinished()){
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                return 0;
            }
            //获取执行失败的job列表
            if(jc.getFailedJobList().size() > 0) {
                System.out.println(jc.getFailedJobList());
                jc.stop();
                return 1;
            }
        }
    }
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int returnStatus = submitJob(args);
        System.exit(returnStatus);
    }
    public static int submitJob(String[] args) throws Exception {
        int returnStatus = ToolRunner.run(new Lab1Driver(), args);
        return returnStatus;
    }

}
