package dq.bigdata.lab1.phase3_pre;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FindMaxMinMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    double max=0;
    double min=0;
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\|");
        double rating=Double.parseDouble(tokens[6]);
        if(Math.abs(rating+9999.0)>1){
            if(rating<min) {
                min = rating;
                context.write(new Text("MIN"),new DoubleWritable(rating));
            }
            if(rating>max) {
                max = rating;
                context.write(new Text("MAX"),new DoubleWritable(rating));
            }
        }
    }

}
