import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoalRegressionMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        try {
            String player = fields[1];
            String year = fields[0];
            int goals = Integer.parseInt(fields[11]);
    
            if (year.equals("2020-21") || year.equals("2021-22")) {
                context.write(new Text(player), new Text(year + ":" + goals));
            }
        } catch (Exception e) {
        }
    }
}
