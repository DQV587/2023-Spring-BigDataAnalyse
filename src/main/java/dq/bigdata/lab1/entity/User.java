package dq.bigdata.lab1.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User
        implements Writable, WritableComparable<User> {
    private final Text review_id=new Text();
    private final DoubleWritable longitude=new DoubleWritable();
    private final DoubleWritable latitude=new DoubleWritable();
    private final DoubleWritable altitude=new DoubleWritable();
    private final Text review_date=new Text();
    private final Text temperature=new Text();
    private final DoubleWritable rating=new DoubleWritable();
    private final Text user_id=new Text();
    private final Text user_birthday=new Text();
    private final Text user_nationality=new Text();
    private final Text user_career=new Text();
    private final DoubleWritable user_income=new DoubleWritable();
    public User(){
        ;
    }

    public Text getReview_id() {
        return review_id;
    }

    public DoubleWritable getLongitude() {
        return longitude;
    }

    public DoubleWritable getLatitude() {
        return latitude;
    }

    public DoubleWritable getAltitude() {
        return altitude;
    }

    public Text getReview_date() {
        return review_date;
    }

    public Text getTemperature() {
        return temperature;
    }

    public DoubleWritable getRating() {
        return rating;
    }

    public Text getUser_id() {
        return user_id;
    }

    public Text getUser_birthday() {
        return user_birthday;
    }

    public Text getUser_nationality() {
        return user_nationality;
    }

    public Text getUser_career() {
        return user_career;
    }

    public DoubleWritable getUser_income() {
        return user_income;
    }
    public void setReview_id(String reviewId){
        review_id.set(reviewId);
    }
    public void setLongitude(double longitude1){
        longitude.set(longitude1);
    }
    public void setLatitude(double latitude1){
        latitude.set(latitude1);
    }
    public void setAltitude(double altitude1){
        altitude.set(altitude1);
    }
    public void setReview_date(String review_date1){
        review_date.set(review_date1);
    }
    public void setTemperature(String temperature1){
        temperature.set(temperature1);
    }
    public void setRating(double rating1){
        rating.set(rating1);
    }
    public void setUser_id(String user_id1){
        user_id.set(user_id1);
    }
    public void setUser_birthday(String user_birthday1){
        user_birthday.set(user_birthday1);
    }
    public void setUser_nationality(String user_nationality1){
        user_nationality.set(user_nationality1);
    }
    public void setUser_career(String user_career1){
        user_career.set(user_career1);
    }
    public void setUser_income(double user_income1){
        user_income.set(user_income1);
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof User)) return false;

        User user = (User) o;

        return new EqualsBuilder().append(review_id, user.review_id).append(longitude, user.longitude).append(latitude, user.latitude).append(altitude, user.altitude).append(review_date, user.review_date).append(temperature, user.temperature).append(rating, user.rating).append(user_id, user.user_id).append(user_birthday, user.user_birthday).append(user_nationality, user.user_nationality).append(user_career, user.user_career).append(user_income, user.user_income).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(review_id).append(longitude).append(latitude).append(altitude).append(review_date).append(temperature).append(rating).append(user_id).append(user_birthday).append(user_nationality).append(user_career).append(user_income).toHashCode();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        review_date.write(dataOutput);
        longitude.write(dataOutput);
        latitude.write(dataOutput);
        altitude.write(dataOutput);
        review_date.write(dataOutput);
        temperature.write(dataOutput);
        rating.write(dataOutput);
        user_id.write(dataOutput);
        user_birthday.write(dataOutput);
        user_nationality.write(dataOutput);
        user_career.write(dataOutput);
        user_income.write(dataOutput);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        review_date.readFields(dataInput);
        longitude.readFields(dataInput);
        latitude.readFields(dataInput);
        altitude.readFields(dataInput);
        review_date.readFields(dataInput);
        temperature.readFields(dataInput);
        rating.readFields(dataInput);
        user_id.readFields(dataInput);
        user_birthday.readFields(dataInput);
        user_nationality.readFields(dataInput);
        user_career.readFields(dataInput);
        user_income.readFields(dataInput);
    }
    @Override
    public String toString() {
        return review_id.toString() + "|" +
                longitude.toString() + "|" +
                latitude.toString() + "|" +
                altitude.toString() + "|" +
                review_date.toString() + "|" +
                temperature.toString() + "|" +
                rating.toString() + "|" +
                user_id.toString() + "|" +
                user_birthday.toString() + "|" +
                user_nationality.toString() + "|" +
                user_career.toString() + "|" +
                user_income.toString();
    }
    @Override
    public int compareTo(User user) {
        int flag=longitude.compareTo(user.getLongitude());
        if(flag==0) return latitude.compareTo(user.getLatitude());
        else return flag;
    }
}
