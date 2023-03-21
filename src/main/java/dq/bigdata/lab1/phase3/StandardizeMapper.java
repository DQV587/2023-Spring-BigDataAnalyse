package dq.bigdata.lab1.phase3;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StandardizeMapper
        extends Mapper<LongWritable, Text, User, NullWritable> {

    private final User user=new User();
    @Override
    protected void map(LongWritable key,Text value,Context context){
        String line = value.toString();
        String[] tokens = line.split("\\|");
        String review_id=tokens[0];
        double longitude= Double.parseDouble(tokens[1]);
        double latitude= Double.parseDouble(tokens[2]);
        double altitude=Double.parseDouble(tokens[3]);
        String date=tokens[4];
        String temperature=tokens[5];
        double rating=Double.parseDouble(tokens[6]);
        String id=tokens[7];
        String birthday=tokens[8];
        String nationality=tokens[9];
        String career=tokens[10];
        double income=Double.parseDouble(tokens[11]);
    }
}
