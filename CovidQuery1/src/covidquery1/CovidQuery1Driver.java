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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

public class CovidQuery1Driver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: CovidQuery1Driver <input> <output>");
            System.exit(-1);
        }

        JobConf job = new JobConf(CovidQuery1Driver.class);
        job.setJobName("COVID_Query1_DeptSexo");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(covidquery1.CovidQuery1Mapper.class);
        job.setReducerClass(covidquery1.CovidQuery1Reducer.class);
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        JobClient.runJob(job);
    }
}
