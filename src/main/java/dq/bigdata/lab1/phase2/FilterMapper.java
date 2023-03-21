package dq.bigdata.lab1.phase2;

import dq.bigdata.lab1.entity.User;
import dq.bigdata.lab1.entity.UserCareer;
import dq.bigdata.lab1.phase1.SampleMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FilterMapper
        extends Mapper<LongWritable, Text, User, NullWritable> {
    private final User user=new User();
    double longitudeMax;
    double longitudeMin;
    double latitudeMax;
    double latitudeMin;
    @Override
    protected void setup(Context context){
        longitudeMax=context.getConfiguration().getDouble("LongitudeMax",11);
        longitudeMin=context.getConfiguration().getDouble("LongitudeMin",8);
        latitudeMax=context.getConfiguration().getDouble("LatitudeMax",57.5);
        latitudeMin=context.getConfiguration().getDouble("LatitudeMin",56.5);
    }


    @Override
    protected void map(LongWritable key,Text value,Context context)
            throws IOException, InterruptedException{
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

        if(longitude<longitudeMax&&longitude>longitudeMin&&latitude<latitudeMax&&latitude>latitudeMin){
            user.setReview_id(review_id);
            user.setLongitude(longitude);
            user.setLatitude(latitude);
            user.setAltitude(altitude);
            user.setReview_date(date);
            user.setTemperature(temperature);
            user.setRating(rating);
            user.setUser_id(id);
            user.setUser_birthday(birthday);
            user.setUser_nationality(nationality);
            user.setUser_career(career);
            user.setUser_income(income);
            context.write(user,NullWritable.get());
        }
    }
}
