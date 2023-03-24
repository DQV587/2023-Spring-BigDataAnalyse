package dq.bigdata.lab1.phase4_pre;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class AvgReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum=0;
        int count=0;
        for(DoubleWritable value:values){
            sum+=value.get();
            count++;
        }
        context.write(key,new DoubleWritable(sum/count));
    }

}
