package dq.bigdata.lab1.phase3;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StandardizeMapper
        extends Mapper<LongWritable, Text, Text, User> {
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    SimpleDateFormat format2 = new SimpleDateFormat("yyyy/MM/dd", Locale.ENGLISH);
    SimpleDateFormat format3 = new SimpleDateFormat("MMMM d,yyyy", Locale.ENGLISH);
    String regex1 = "[0-9]{4}-[0-9]{2}-[0-9]{2}";
    Pattern pattern1 = Pattern.compile(regex1);
    String regex2 = "[0-9]{4}/[0-9]{2}/[0-9]{2}";
    Pattern pattern2 = Pattern.compile(regex2);
    String regex3 = "[a-zA-Z]+ [0-9]+,[0-9]{4}";
    Pattern pattern3 = Pattern.compile(regex3);

    String temperatureReg1="(-?[0-9]+\\.[0-9]+)(℉)";
    Pattern temperaturePattern1=Pattern.compile(temperatureReg1);
    String numberReg="-?[0-9]+\\.[0-9]+";
    Pattern numberPattern=Pattern.compile(numberReg);
    private final User user=new User();
    @Override
    protected void map(LongWritable key,Text value,Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\\|");
        String review_id=tokens[0];
        double longitude=Double.parseDouble(tokens[1]);
        double latitude=Double.parseDouble(tokens[2]);
        double altitude=Double.parseDouble(tokens[3]);
        String date=tokens[4];
        String temperature=tokens[5];

        if(temperaturePattern1.matcher(temperature).matches()){
            Matcher numMatcher=numberPattern.matcher(temperature);
            if(numMatcher.find()){
                temperature= String.format("%.1f", (Double.parseDouble(numMatcher.group())-32)/1.8) +"℃";
            }
        }
        double rating=Double.parseDouble(tokens[6]);
        String id=tokens[7];
        String birthday=tokens[8];

        try {
            if (pattern1.matcher(date).matches()) {

            }
            else if (pattern2.matcher(date).matches()) {
                date = format1.format(format2.parse(date));
            }
            else if (pattern3.matcher(date).matches()) {
                date = format1.format(format3.parse(date));
            }

            if (pattern1.matcher(birthday).matches()) {
            }
            else if (pattern2.matcher(birthday).matches()) {
                birthday = format1.format(format2.parse(birthday));
            }
            else if (pattern3.matcher(birthday).matches()) {
                birthday = format1.format(format3.parse(birthday));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        String nationality=tokens[9];
        String career=tokens[10];
        double income=Double.parseDouble(tokens[11]);

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
