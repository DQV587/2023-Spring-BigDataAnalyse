package dq.bigdata.lab1.phase3;

import dq.bigdata.lab1.entity.User;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StandardizeReducer
        extends Reducer<Text, User, User, NullWritable> {
    double ratingMax;
    double ratingMin;
    @Override
    protected void setup(Context context){
        ratingMax=context.getConfiguration().getDouble("RatingMax",200);
        ratingMin=context.getConfiguration().getDouble("RatingMin",-200);
    }
    @Override
    protected void reduce(Text key, Iterable<User> values, Context context) throws IOException, InterruptedException {
        for(User user:values){
            double rating=user.getRating().get();
            if(Math.abs(rating+9999)>1e-5)
                user.setRating((rating-ratingMin)/(ratingMax-ratingMin));
            context.write(user,NullWritable.get());
        }
    }
}
