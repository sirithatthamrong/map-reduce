package StageTwo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StageTwoReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        for (Text val : values) {
            sum += Double.parseDouble(val.toString().split(" ")[0]); // Extracting the sum from the value
        }
        context.write(key, new Text(sum + " C")); // Adding 'C' to indicate the matrix name
    }
}
