package dq.bigdata.lab1.phase2;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterMapper
        extends Mapper<LongWritable, Text, Text, User> {
}
