package StageTwo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StageTwoMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        int row = Integer.parseInt(tokens[0]);
        int col = Integer.parseInt(tokens[1]);
        context.write(new Text(row + " " + col), new Text(tokens[2]));
    }
}
