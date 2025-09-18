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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CovidQuery5Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return; // Skip header
        
        String[] fields = value.toString().split(";");
        if (fields.length >= 10) {
            try {
                String departamento = fields[5].trim();
                double edad = Double.parseDouble(fields[2].trim());
                if (edad > 0 && edad < 150) {
                    output.collect(new Text(departamento), new DoubleWritable(edad));
                }
            } catch (NumberFormatException e) {
                // Ignorar valores no numéricos
            }
        }
    }
}
