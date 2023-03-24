package dq.bigdata.lab1.phase4;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.base.Charsets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;


public class FillMapper
        extends Mapper<LongWritable, Text, Text, User> {

    enum FillCounter{
        Rating,Income
    }
    private final User user=new User();
    private final Map<String,Double> incomeMap=new HashMap<>();
    private final Map<String ,Double> ratingMap=new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        String path = "/lab1/phase4/avgIncome";
        Path file=new Path(path,"part-r-00000");
        if (!fs.exists(file)) {
            throw new IOException("Output not found!");
        }else {
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(file),Charsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                String[] keyword = line.split("\\s+");
                Double fillValue = Double.valueOf((keyword[1].substring(0, 6)));
                incomeMap.put(keyword[0],fillValue);
            }
            br.close();
        }
        String path1 = "/lab1/phase4/avgRating";
        Path file1=new Path(path,"part-r-00000");
        if (!fs.exists(file1)) {
            throw new IOException("Output not found!");
        }else {
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(file1),Charsets.UTF_8));
            String line;
            while ((line = br.readLine()) != null) {
                String[] keyword = line.split("\\s+");
                Double fillValue = Double.valueOf((keyword[1]));
                ratingMap.put(keyword[0],fillValue);
            }
            br.close();
        }
    }
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
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
        if(Math.abs(income+9999)<1e-4) {
            income = incomeMap.get(nationality + "|" + career);
            context.getCounter(FillCounter.Income).increment(1L);
        }
        if(Math.abs(rating+9999)<1e-4){
            rating=ratingMap.get(nationality + "|" + career);
            context.getCounter(FillCounter.Rating).increment(1L);
        }

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
