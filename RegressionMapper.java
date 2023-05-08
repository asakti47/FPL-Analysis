import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.util.Arrays;


public class RegressionMapper extends Mapper<LongWritable, Text, Text, Text> {

    ArrayList<String> header = new ArrayList<String>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        header.addAll(Arrays.asList("Year", "Player", "Nation", "Pos", "Squad", "Age", "Born", "MP", "Starts", "Min", "90s", "Gls",
         "Ast", "Pk", "PKatt", "CrdY", "CrdR", "xG", "npxg", "xAG", "PrgC", "PrgP", "PrgR"));

        if (key.get() == 0) {
            return; // Skip header line
        }

        String[] columns = value.toString().split(",");
        try {
            double column12Value = Double.parseDouble(columns[11]);

            for (int i = 0; i < columns.length; i++) {
                if (i != 11) {
                    try {
                        double columnValue = Double.parseDouble(columns[i]);
                        context.write(new Text(header.get(i)), new Text(column12Value + "," + columnValue));
                    } catch (Exception e) {
                    }
                }
            }
        } catch (Exception e) {
        }
    }
}