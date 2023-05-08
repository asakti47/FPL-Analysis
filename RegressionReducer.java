import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RegressionReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Double> column12 = new ArrayList<>();
        List<Double> currentColumn = new ArrayList<>();

        for (Text value : values) {
            String[] valuePair = value.toString().split(",");
            try {
                double column12Value = Double.parseDouble(valuePair[0]);
                double currentColumnValue = Double.parseDouble(valuePair[1]);
                column12.add(column12Value);
                currentColumn.add(currentColumnValue);
            } catch (Exception e) {
            }
        }

        SimpleRegression regression = new SimpleRegression();
        for (int i = 0; i < column12.size(); i++) {
            regression.addData(column12.get(i), currentColumn.get(i));
        }

        String regressionResult = "slope: " + regression.getSlope() + ", intercept: " + regression.getIntercept();
        context.write(key, new Text(regressionResult));
    }
}
