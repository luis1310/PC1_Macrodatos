/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package covidquery1;

/**
 *
 * @author luisdelacruzmantilla
 */

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CovidQuery1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return; // Skip header
        
        String[] fields = value.toString().split(";");
        if (fields.length >= 10) {
            String departamento = fields[5].trim();
            String sexo = fields[3].trim();
            
            if (!departamento.isEmpty() && !sexo.isEmpty()) {
                output.collect(new Text(departamento + "," + sexo), one);
            }
        }
    }
}
