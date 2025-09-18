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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class CovidQuery6Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: CovidQuery6Driver <input> <output_base>");
            System.exit(-1);
        }
        
        String inputPath = args[0];
        String outputBase = args[1];
        String phase1Output = outputBase + "_phase1";
        String phase2Output = outputBase + "_phase2";
        
        // EJECUTAR FASE 1
        JobConf job1 = ejecutarFase1(inputPath, phase1Output);

        // EJECUTAR FASE 2
        JobConf job2 = ejecutarFase2(phase1Output, phase2Output);

        // Generar resumen final
        SummaryGenerator.generateSummary(phase1Output, phase2Output);
        
        System.out.println("Consulta 6 completada. Resultados en: " + phase2Output);
    }
    
    private static JobConf ejecutarFase1(String input, String output) throws IOException {
        JobConf job = new JobConf(CovidQuery6Driver.class);
        job.setJobName("COVID_Query6_Phase1");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(covidquery6.CovidQuery6Phase1Mapper.class);
        job.setReducerClass(covidquery6.CovidQuery6Phase1Reducer.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        JobClient.runJob(job);
        System.out.println("Fase 1 completada");
        return job;
    }
    
    private static JobConf ejecutarFase2(String input, String output) throws IOException {
        JobConf job = new JobConf(CovidQuery6Driver.class);
        job.setJobName("COVID_Query6_Phase2");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(covidquery6.CovidQuery6Phase2Mapper.class);
        job.setReducerClass(covidquery6.CovidQuery6Phase2Reducer.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        // Configurar ordenamiento personalizado por densidad
        job.setOutputKeyComparatorClass(covidquery6.DensidadComparator.class);
        
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        JobClient.runJob(job);
        System.out.println("Fase 2 completada");
        return job;
    }
}