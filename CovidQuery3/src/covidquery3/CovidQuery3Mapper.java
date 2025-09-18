/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package covidquery3;

/**
 *
 * @author luisdelacruzmantilla
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CovidQuery3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private String searchTerm = "COVID";

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return; // Skip header
        
        String line = value.toString().toLowerCase();
        String[] fields = line.split(";");
        
        if (fields.length >= 10) {
            boolean found = false;
            // Buscar en CLASIFICACION_DEF(4), DEPARTAMENTO(5), PROVINCIA(6), DISTRITO(7)
            for (int i = 4; i <= 7; i++) {
                if (fields[i].contains(searchTerm.toLowerCase())) {
                    found = true;
                    break;
                }
            }
            
            if (found) {
                // Emitir el registro completo, no solo contar
                output.collect(new Text("registro_completo"), new Text(value.toString()));
            }
        }
    }
}