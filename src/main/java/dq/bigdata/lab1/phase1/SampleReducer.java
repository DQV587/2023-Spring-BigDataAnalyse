package dq.bigdata.lab1.phase1;

import dq.bigdata.lab1.entity.User;
import dq.bigdata.lab1.entity.UserCareer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SampleReducer
        extends Reducer<Text, User, Text, NullWritable> {
    @Override
    protected void reduce(Text key,Iterable<User> values,Context context) throws IOException, InterruptedException {
        double ratio=context.getConfiguration().getDouble("Ratio",0.4);
        for(User user:values){
            if(Math.random()<=ratio){
                context.write(new Text(user.toString()),NullWritable.get());
            }
        }
    }
}
