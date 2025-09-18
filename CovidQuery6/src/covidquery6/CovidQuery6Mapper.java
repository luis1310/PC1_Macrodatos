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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

// MAPPER FASE 1
class CovidQuery6Phase1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return;
        
        String[] fields = value.toString().split(";");
        if (fields.length >= 10) {
            try {
                double edad = Double.parseDouble(fields[2].trim());
                String departamento = fields[5].trim();
                String fechaFallecimiento = fields[1].trim();
                
                if (edad > 0 && edad < 150 && fechaFallecimiento.length() == 8) {
                    Date fecha = dateFormat.parse(fechaFallecimiento);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(fecha);
                    
                    int year = cal.get(Calendar.YEAR);
                    int rangoEdad = (int)(edad / 10) * 10;
                    
                    String key_output = departamento + "_" + year + "_" + rangoEdad;
                    output.collect(new Text(key_output), new DoubleWritable(edad));
                }
            } catch (NumberFormatException | ParseException e) {
                // Ignorar valores inválidos
            }
        }
    }
}

// MAPPER FASE 2
class CovidQuery6Phase2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("\t");
        if (parts.length >= 2) {
            String keyPart = parts[0];
            String valuePart = parts[1];
            
            String[] keyComponents = keyPart.split("_");
            String[] valueComponents = valuePart.split(",");
            
            if (keyComponents.length >= 3 && valueComponents.length >= 5) {
                String departamento = keyComponents[0];
                double densidad = Double.parseDouble(valueComponents[1]);
                
                String clasificacion;
                if (densidad > 0.5) {
                    clasificacion = "ALTA_DENSIDAD";
                } else if (densidad > 0.1) {
                    clasificacion = "MEDIA_DENSIDAD";
                } else {
                    clasificacion = "BAJA_DENSIDAD";
                }
                
                output.collect(new Text(clasificacion), new Text(departamento + ":" + valuePart));
            }
        }
    }
}

// Clase principal del archivo (solo sera un place holder)
public class CovidQuery6Mapper {
    // no retorna nada
}