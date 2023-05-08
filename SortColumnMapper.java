import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortColumnMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");

        if (columns.length >= 2) {
            double secondColumn = Double.parseDouble(columns[1]);
            context.write(new DoubleWritable(-secondColumn), new Text(columns[0]));
        }
    }
}
