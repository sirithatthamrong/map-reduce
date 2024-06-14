import StageOne.*;
import StageTwo.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplication {
    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiplication <input path> <output1 path> <output2 path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // First MapReduce job
        Job job1 = Job.getInstance(conf, "Matrix Multiplication Stage One");
        job1.setJarByClass(MatrixMultiplication.class);
        job1.setMapperClass(StageOneMapper.class);
        job1.setReducerClass(StageOneReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));

//        job1.waitForCompletion(true);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        if (!job1.isSuccessful()) {
            System.err.println("Stage One Job failed!");
            System.exit(1);
        }

        // Second MapReduce job
        Job job2 = Job.getInstance(conf, "Matrix Multiplication Stage Two");
        job2.setJarByClass(MatrixMultiplication.class);
        job2.setMapperClass(StageTwoMapper.class);
        job2.setReducerClass(StageTwoReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job2, new Path(args[1]));
        TextOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}