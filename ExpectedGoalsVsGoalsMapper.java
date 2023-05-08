import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExpectedGoalsVsGoalsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        try {
            String line = value.toString();
            String[] columns = line.split(",");
    
            if (columns.length >= 18) {
                String secondColumn = columns[1];
                String result = (Double.parseDouble(columns[11]) - Double.parseDouble(columns[17])) + "";
                context.write(new Text(secondColumn), new Text(result));
            }
        }
        catch (Exception e) {
        }

    }
}