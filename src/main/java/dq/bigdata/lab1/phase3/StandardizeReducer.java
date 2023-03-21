package dq.bigdata.lab1.phase3;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class StandardizeReducer
        extends Reducer<User, NullWritable, User, NullWritable> {
}
