package StageOne;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class StageOneReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<Integer, Double> mapA = new HashMap<>();
        HashMap<Integer, Double> mapB = new HashMap<>();

        for (Text val : values) {
            String[] tokens = val.toString().split(",");
            String matrixName = tokens[0];
            int index = Integer.parseInt(tokens[1]);
            double value = Double.parseDouble(tokens[2]);

            if (matrixName.equals("A")) {
                mapA.put(index, value);
            } else if (matrixName.equals("B")) {
                mapB.put(index, value);
            }
        }

        for (int i : mapA.keySet()) {
            for (int j : mapB.keySet()) {
                double product = mapA.get(i) * mapB.get(j);
                context.write(new Text(i + " " + j), new Text(product + " C"));
            }
        }
    }
}


