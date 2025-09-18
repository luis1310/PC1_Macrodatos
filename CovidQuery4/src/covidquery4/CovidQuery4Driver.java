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
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

public class CovidQuery4Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: CovidQuery4Driver <input> <output>");
            System.exit(-1);
        }

        JobConf job = new JobConf(CovidQuery4Driver.class);
        job.setJobName("COVID_Query4_DateRange");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(covidquery4.CovidQuery4Mapper.class);
        job.setReducerClass(covidquery4.CovidQuery4Reducer.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        JobClient.runJob(job);
    }
}