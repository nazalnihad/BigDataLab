import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {
    
    public static class MatrixMapper 
        extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            //  the filename to determine which matrix we're processing
            FileSplit split = (FileSplit) context.getInputSplit();
            String filename = split.getPath().getName();
            
            String line = value.toString();
            String[] parts = line.split(",");
            
            // Expected format: i,j,value for each matrix element
            if (parts.length == 3) {
                int i = Integer.parseInt(parts[0].trim());
                int j = Integer.parseInt(parts[1].trim());
                double val = Double.parseDouble(parts[2].trim());
                
                if (filename.startsWith("matrix_a")) {
                    for (int k = 0; k < context.getConfiguration().getInt("matrix.size", 3); k++) {
                        outputKey.set(i + "," + k);
                        outputValue.set("A," + j + "," + val);
                        context.write(outputKey, outputValue);
                    }
                } else {
                    for (int k = 0; k < context.getConfiguration().getInt("matrix.size", 3); k++) {
                        outputKey.set(k + "," + j);
                        outputValue.set("B," + i + "," + val);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }
    
    public static class MatrixReducer 
        extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            // Store matrix elements
            List<MatrixElement> elementListA = new ArrayList<>();
            List<MatrixElement> elementListB = new ArrayList<>();
            
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts[0].equals("A")) {
                    elementListA.add(new MatrixElement(
                        Integer.parseInt(parts[1]), 
                        Double.parseDouble(parts[2])
                    ));
                } else {
                    elementListB.add(new MatrixElement(
                        Integer.parseInt(parts[1]), 
                        Double.parseDouble(parts[2])
                    ));
                }
            }
            
            double sum = 0.0;
            for (MatrixElement a : elementListA) {
                for (MatrixElement b : elementListB) {
                    if (a.index == b.index) {
                        sum += a.value * b.value;
                    }
                }
            }
            
            if (sum != 0.0) {
                result.set(String.valueOf(sum));
                context.write(key, result);
            }
        }
    }
    
    private static class MatrixElement {
        int index;
        double value;
        
        MatrixElement(int index, double value) {
            this.index = index;
            this.value = value;
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiplication <matrix size> <input path> <output path>");
            System.exit(-1);
        }
        
        Configuration conf = new Configuration();
        conf.setInt("matrix.size", Integer.parseInt(args[0]));
        
        Job job = Job.getInstance(conf, "matrix multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}