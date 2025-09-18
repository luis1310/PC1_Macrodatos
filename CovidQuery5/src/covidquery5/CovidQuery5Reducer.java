/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package covidquery5;

/**
 *
 * @author luisdelacruzmantilla
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CovidQuery5Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        int count = 0;
        
        while (values.hasNext()) {
            double edad = values.next().get();
            if (edad < min) min = edad;
            if (edad > max) max = edad;
            count++;
        }
        
        String resultado = String.format("Min: %.0f, Max: %.0f, Total: %d", min, max, count);
        output.collect(key, new Text(resultado));
    }
}
