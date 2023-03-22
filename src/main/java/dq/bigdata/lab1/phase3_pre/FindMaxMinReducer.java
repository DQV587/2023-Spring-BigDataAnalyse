package dq.bigdata.lab1.phase3_pre;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FindMaxMinReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    double min=0;
    double max=0;
    @Override
    protected void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException, InterruptedException {
        if(key.toString().equals("MIN")){
            for(DoubleWritable value:values){
                if(value.get()<min)
                    min=value.get();
            }
            context.write(new Text("MIN"),new DoubleWritable(min));
        }
        if(key.toString().equals("MAX")){
            for(DoubleWritable value:values){
                if(value.get()>max)
                    max=value.get();
            }
            context.write(new Text("MAX"),new DoubleWritable(max));
        }
    }
}
