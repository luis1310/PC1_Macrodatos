/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package covidquery4;

/**
 *
 * @author luisdelacruzmantilla
 */

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CovidQuery4Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        int count = 0;
        while (values.hasNext()) {
            Text registro = values.next();
            count++;
            // Emitir cada registro completo con número de secuencia
            output.collect(new Text("registro_" + count), registro);
        }
        
        // También emitir el total de registros en el rango
        output.collect(new Text("total_en_rango"), new Text(String.valueOf(count)));
    }
}