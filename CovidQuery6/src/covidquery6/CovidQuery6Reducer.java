/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package covidquery6;
/**
 *
 * @author luisdelacruzmantilla
 */
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

// REDUCER FASE 1 - Para manejar DoubleWritable
class CovidQuery6Phase1Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {
    
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        double suma = 0;
        int count = 0;
        double max = Double.MIN_VALUE;
        double min = Double.MAX_VALUE;
        
        while (values.hasNext()) {
            double edad = values.next().get();
            suma += edad;
            count++;
            if (edad > max) max = edad;
            if (edad < min) min = edad;
        }
        
        if (count > 0) {
            double promedio = suma / count;
            double densidad = count / (max - min + 1);
            
            String resultado = String.format("%.2f,%.4f,%d,%.0f,%.0f", promedio, densidad, count, min, max);
            output.collect(key, new Text(resultado));
        }
    }
}

// REDUCER FASE 2 - Para manejar Text
class CovidQuery6Phase2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        
        StringBuilder resultado = new StringBuilder();
        int count = 0;
        
        while (values.hasNext()) {
            if (count > 0) {
                resultado.append(";\n    "); // Agregar salto de línea e indentación
            }
            resultado.append(values.next().toString());
            count++;
        }
        
        // Formato más legible con salto de línea al final
        String salida = resultado.toString() + "\n    [Total: " + count + " grupos de edades de departamentos]";
        output.collect(key, new Text(salida));
    }
}


// clase apra calcular metricas

class SummaryGenerator {
    public static void generateSummary(String fase1Output, String fase2Output) {
        try {
            System.out.println("consulta 6: resultados");
            
            // Leer y analizar resultados de Fase 1
            int totalGrupos = 0;
            int totalFallecidos = 0;
            int gruposAltaDensidad = 0;
            int gruposMediaDensidad = 0;
            int gruposBajaDensidad = 0;
            double maxDensidad = 0;
            String grupoMaxDensidad = "";
            
            System.out.println("ANÁLISIS FASE 1:");
            System.out.println("- Archivo de entrada procesado: " + fase1Output);
            
            // Leer y analizar resultados de Fase 2
            System.out.println("\nANÁLISIS FASE 2:");
            System.out.println("- Archivo de salida: " + fase2Output);
            
        } catch (Exception e) {
            System.err.println("Error generando resumen: " + e.getMessage());
        }
    }
}

// Clase principal del archivo
public class CovidQuery6Reducer {
    // no retorna nada
}