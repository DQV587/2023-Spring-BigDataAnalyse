package dq.bigdata.lab1.phase4_pre;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RegressionMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key,Text value,Context context){
        String line = value.toString();
        String[] tokens = line.split("\\|");
        double longitude= Double.parseDouble(tokens[1]);
        double latitude= Double.parseDouble(tokens[2]);
        double altitude=Double.parseDouble(tokens[3]);
        double rating=Double.parseDouble(tokens[6]);
        String nationality=tokens[9];
        String career=tokens[10];
        double income=Double.parseDouble(tokens[11]);
    }
}
