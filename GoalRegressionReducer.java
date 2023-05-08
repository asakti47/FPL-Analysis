import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GoalRegressionReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int totalGoals2020 = 0;
        int totalGoals2021 = 0;

        for (Text value : values) {
            String[] fields = value.toString().split(":");
            String year = fields[0];
            int goals = Integer.parseInt(fields[1]);

            if (year.equals("2020-21")) {
                totalGoals2020 += goals;
            } else if (year.equals("2021-22")) {
                totalGoals2021 += goals;
            }
        }

        int regression = totalGoals2021 - totalGoals2020;
        context.write(key, new IntWritable(regression));
    }
}
