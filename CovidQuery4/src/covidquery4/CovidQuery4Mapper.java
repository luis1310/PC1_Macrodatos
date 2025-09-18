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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class CovidQuery4Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private String startDate = "20220101"; // Rango de inicio para fecha de fallecimiento
    private String endDate = "20221231"; // Rango de fin para fecha de fallecimiento

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) return; // Skip header
        
        String[] fields = value.toString().split(";");
        if (fields.length >= 10) {
            try {
                String fechaFallecimiento = fields[1].trim();
                
                if (fechaFallecimiento.length() != 8 || !fechaFallecimiento.matches("\\d{8}")) {
                    return;
                }
                
                Date fecha = dateFormat.parse(fechaFallecimiento);
                Date inicio = dateFormat.parse(startDate);
                Date fin = dateFormat.parse(endDate);
                
                if (fecha.compareTo(inicio) >= 0 && fecha.compareTo(fin) <= 0) {
                    // Emitir el registro completo
                    output.collect(new Text("registro_en_rango"), new Text(value.toString()));
                }
            } catch (ParseException e) {
                // Ignorar fechas mal formateadas
            }
        }
    }
}