package dq.bigdata.lab1.phase2;
import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterReducer
        extends Reducer<User, NullWritable, User, NullWritable>{
    @Override
    protected void reduce(User key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
