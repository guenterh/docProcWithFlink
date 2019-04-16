package org.swissbib.docproc.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

//import org.swissbib.





/**
 * Created by swissbib on 08.05.17.
 */
public class DocProcEngine {

    public static void main (String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);
        ParameterTool parameters = ParameterTool.fromPropertiesFile("data/config.us-13.properties");
        env.getConfig().setGlobalJobParameters(parameters);


        DataSource<String> docProcRecords = env.readTextFile("data/job1r127A149.format.xml.gz");

        docProcRecords.map(new MyDocProc1MapFunction()).withParameters(parameters.getConfiguration())
                .writeAsText("dataout/output.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute("swissbib classic goes BigData");


    }



    public static class MyDocProc1MapFunction extends RichMapFunction<String, String> {


        @Override
        public String map(String record) throws Exception {

            //todo make transformations
            return record;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            //XSLTPipeStart pipeStart = new XSLTPipeStart();
            //pipeStart.setConfigFile("pipeConfig.test1.yaml");
            //pipeStart.setTransformerFactory("net.sf.saxon.TransformerFactoryImpl");
            //MFXsltBasedBridge

            super.open(parameters);
        }



    }


}
