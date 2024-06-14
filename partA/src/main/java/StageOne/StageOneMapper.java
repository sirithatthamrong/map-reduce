package StageOne;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StageOneMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\s+");
        int row = Integer.parseInt(tokens[0]);
        int col = Integer.parseInt(tokens[1]);
        double val = Double.parseDouble(tokens[2]);
        String matrixName = tokens[3];

        if (matrixName.equals("A")) {
            context.write(new Text(Integer.toString(col)), new Text("A," + row + "," + val));
        } else if (matrixName.equals("B")) {
            context.write(new Text(Integer.toString(row)), new Text("B," + col + "," + val));
        }
    }
}
