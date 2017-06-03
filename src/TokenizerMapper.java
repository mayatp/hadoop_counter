import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> { 

    private IntWritable one = new IntWritable(1);
    private IntWritable tweet_len = new IntWritable();
    
    // Sample input
    // epoch_time [0];tweetId [1];tweet(hashtag contained)[2]; device [3]
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        //1: Find feature and partial value, splits the line into strings
        
        String[] line = value.toString().split(";");
	
	// checks if the line does actually contain a tweet and forms groups with aggregation of 5
        if (line.length == 4) {
        	int tweet = line[2].length();
        	int group = (tweet - tweet%5 + 5);     	
        	tweet_len.set(group);
        }
        context.write(tweet_len, one);
    }
}
