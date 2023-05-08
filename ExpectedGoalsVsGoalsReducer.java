import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExpectedGoalsVsGoalsReducer extends Reducer<Text, Text, Text, Text> {
    private Text outputKey = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        for (Text value : values) {
            sum += Double.parseDouble(value.toString());
        }
        outputKey.set(key.toString() + ",");
        context.write(outputKey, new Text(String.valueOf(sum)));
    }
}