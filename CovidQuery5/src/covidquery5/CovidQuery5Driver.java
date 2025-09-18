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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CovidQuery5Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: CovidQuery5Driver <input> <output>");
            System.exit(-1);
        }

        JobConf job = new JobConf(CovidQuery5Driver.class);
        job.setJobName("COVID_Query5_MinMax");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapperClass(covidquery5.CovidQuery5Mapper.class);
        job.setReducerClass(covidquery5.CovidQuery5Reducer.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        JobClient.runJob(job);
    }
}
