package dq.bigdata.lab1.phase1;

import dq.bigdata.lab1.entity.User;
import dq.bigdata.lab1.entity.UserCareer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SampleMapper
        extends Mapper<LongWritable, Text, Text, User> {
    enum MissingValue{
        rating,
        user_income;
    }
    private final User user=new User();
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
        double rating;
        if(tokens[6].equals("?")){
            rating= (double) -9999;
            context.getCounter(MissingValue.rating).increment(1L);
        }
        else rating=Double.parseDouble(tokens[6]);
        String id=tokens[7];
        String birthday=tokens[8];
        String nationality=tokens[9];
        String career="";
        switch (tokens[10]){
            case "programmer":
                context.getCounter(UserCareer.PROGRAMMER).increment(1L);
                career="programmer";
                break;
            case "teacher":
                context.getCounter(UserCareer.TEACHER).increment(1L);
                career="teacher";
                break;
            case "farmer":
                context.getCounter(UserCareer.FARMER).increment(1L);
                career="farmer";
                break;
            case "doctor":
                context.getCounter(UserCareer.DOCTOR).increment(1L);
                career="doctor";
                break;
            case "Manager":
                context.getCounter(UserCareer.MANAGER).increment(1L);
                career="manager";
                break;
            case "accountant":
                context.getCounter(UserCareer.ACCOUNTANT).increment(1L);
                career="accountant";
                break;
            case "artist":
                context.getCounter(UserCareer.ARTIST).increment(1L);
                career="artist";
                break;
            case "writer":
                context.getCounter(UserCareer.WRITER).increment(1L);
                career="writer";
                break;
        }
        double income;
        if(tokens[11].equals("?")){
            income=-9999;
            context.getCounter(MissingValue.user_income).increment(1L);
        }
        else income=Double.parseDouble(tokens[11]);
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
        context.write(new Text(career),user);
    }
}
