package org.swissbib.docproc.flink;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.HashMap;


/**
 * Created by swissbib on 08.05.17.
 */
public class DocProcEngine {

    public static void main (String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(1);
        ParameterTool parameters = ParameterTool.fromPropertiesFile("data/configGreen.properties");
        env.getConfig().setGlobalJobParameters(parameters);


        DataSource<String> docProcRecords = env.readTextFile("data/job1r127A149.format.xml.gz");

        docProcRecords.map(new MyDocProc1MapFunction()).withParameters(parameters.getConfiguration())
                .writeAsText("dataout/output.txt", FileSystem.WriteMode.OVERWRITE);
        env.execute("Flink Java API Skeleton");


        /**
         * Here, you can start creating your execution plan for Flink.
         *
         * Start with getting some data from the environment, like
         * 	env.readTextFile(textPath);
         *
         * then, transform the resulting DataSet<String> using operations
         * like
         * 	.filter()
         * 	.flatMap()
         * 	.join()
         * 	.coGroup()
         * and many more.
         * Have a look at the programming guide for the Java API:
         *
         * http://flink.apache.org/docs/latest/programming_guide.html
         *
         * and the examples
         *
         * http://flink.apache.org/docs/latest/examples.html
         *
         */

        // execute program




    }


    public static class MyDocProcMapFunction implements MapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            String tDoc = "";
            if (s.length() > 3)
                tDoc = s.substring(0,3);

            return tDoc;
        }
    }




    public static class MyDocProc1MapFunction extends RichMapFunction<String, String> {

        @Override
        public String map(String s) throws Exception {
            return s;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            String allParams = parameters.getString("ALL_PROPERTIES","");
            String[] propKeys = allParams.split("###");

            HashMap<String,String> config = new HashMap<>();
            Arrays.asList(propKeys).forEach((String key) ->
                    {
                        config.put(key, parameters.getString(key,""));

                    }
            );

            super.open(parameters);
        }
    }
}
