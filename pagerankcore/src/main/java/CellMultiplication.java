
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CellMultiplication {

    public static class TransiitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input: 1   2,3,5,7,12
            // output: <1, 2=1/5>, <1, 3=1/5>, <1, 5=1/5>...
            String fId = value.toString().split("\t")[0];
            String[] tos = value.toString().split("\t")[1].split(",");

            for(String to : tos) {
                context.write(new Text(fId), new Text(to + "=" + (double)1/tos.length));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // input: 1\t1 / 2\t1 / 3\t1...
            // output: 1 1, 2 1, 3 1...

            String[] words = value.toString().split("\t");
            context.write(new Text(words[0]), new Text(words[1]));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }


    public static void main(String[] args) {

    }
}
