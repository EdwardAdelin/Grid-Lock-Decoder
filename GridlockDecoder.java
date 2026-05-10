import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GridlockDecoder {

    public static class TaxiMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        private Text hourKey = new Text();
        private DoubleWritable durationValue = new DoubleWritable();
        private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        private SimpleDateFormat hourFormat = new SimpleDateFormat("HH");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.contains("VendorID")) return; // Skip the header row

            String[] columns = line.split(",");
            if (columns.length > 2) {
                try {
                    Date pickup = format.parse(columns[1]);
                    Date dropoff = format.parse(columns[2]);
                    
                    // Extract hour (e.g., "08")
                    String hour = hourFormat.format(pickup);
                    
                    // Calculate duration in minutes
                    double minutes = (dropoff.getTime() - pickup.getTime()) / 60000.0;

                    if (minutes > 0 && minutes < 600) {
                        hourKey.set(hour + ":00"); // Format as "08:00"
                        durationValue.set(minutes);
                        context.write(hourKey, durationValue); // Send to Reducer
                    }
                } catch (Exception e) { /* Ignore bad data rows */ }
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            if (count > 0) {
                double avg = Math.round((sum / count) * 100.0) / 100.0; // Round to 2 decimals
                result.set(avg);
                context.write(key, result); // Final output
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gridlock Decoder");
        job.setJarByClass(GridlockDecoder.class);
        job.setMapperClass(TaxiMapper.class);
        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}