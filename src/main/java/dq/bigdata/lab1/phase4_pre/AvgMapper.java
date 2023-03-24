package dq.bigdata.lab1.phase4_pre;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AvgMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key ,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\|");
        String nationality=tokens[9];
        String career=tokens[10];
        double rating=Double.parseDouble(tokens[6]);
        if(Math.abs(rating+9999.0)>1e-4){
            context.write(new Text(nationality+"|"+career),new DoubleWritable(rating));
        }
    }
}
