package dq.bigdata.lab1.phase1;


import dq.bigdata.lab1.entity.User;
import dq.bigdata.lab1.entity.UserCareer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SamplePartitioner extends Partitioner<Text, User> {

    @Override
    public int getPartition(Text text, User user, int i) {
        String key=text.toString();
        switch (key){
            case "teacher":
                return 1%i;
            case "farmer":
                return 2%i;
            case "doctor":
                return 3%i;
            case "manager":
                return 4%i;
            case "accountant":
                return 5%i;
            case "artist":
                return 6%i;
            case "writer":
                return 7%i;
            default:
                return 0;
        }
    }
}
