/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package covidquery2;

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

public class CovidQuery2Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {

    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        java.util.List<Double> edades = new java.util.ArrayList<>();
        double suma = 0;
        int count = 0;
        
        while (values.hasNext()) {
            double edad = values.next().get();
            edades.add(edad);
            suma += edad;
            count++;
        }
        
        double promedio = suma / count;
        
        java.util.Collections.sort(edades);
        double mediana = edades.size() % 2 == 0 ? 
            (edades.get(edades.size()/2 - 1) + edades.get(edades.size()/2)) / 2.0 :
            edades.get(edades.size()/2);
        
        double sumaCuadrados = 0;
        for (double edad : edades) {
            sumaCuadrados += Math.pow(edad - promedio, 2);
        }
        double desviacion = Math.sqrt(sumaCuadrados / count);
        
        String resultado = String.format("\nPromedio: %.2f, \nMediana: %.2f, \nDesviación: %.2f, \nCount: %d\n", 
            promedio, mediana, desviacion, count);
        
        output.collect(key, new Text(resultado));
    }
}
