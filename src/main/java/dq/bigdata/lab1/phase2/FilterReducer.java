package dq.bigdata.lab1.phase2;
import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer
        extends Reducer<Text, User, User,NullWritable>{
    @Override
    protected void reduce(Text key,Iterable<User> values,Context context) throws IOException, InterruptedException {
        for(User user:values){
            context.write(user,NullWritable.get());
        }

    }
}
