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
import org.swissbib.docproc.flink.plugins.IDocProcPlugin;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Pattern;


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

        protected static ArrayList <IDocProcPlugin> plugins ;
        protected ArrayList<Transformer> transformers = new ArrayList<Transformer>();

        protected TransformerFactory transformerFactory = null;
        private static HashMap<String,String> config = new HashMap<>();

        private ArrayList<String> xpathDirs;
        protected Transformer weedingsTransformer;
        protected Transformer holdingsTransformer;

        final Pattern pProcessRecord = Pattern.compile("^<record");


        static {
            plugins = new ArrayList<>();
        }



        @Override
        public String map(String record) throws Exception {

            if (!this.pProcessRecord.matcher(record).find()) {
                return record;
            }

            StringWriter finalRecord = new StringWriter();


            Source sourceHoldings = new StreamSource(new StringReader(record));
            Source sourceWeeding = new StreamSource(new StringReader(record));

            StringWriter holdings = new StringWriter();
            StringWriter weededRecord = new StringWriter();


            //we fetch all the holdings of a bibliographic record
            Result xsltResultHoldings = new StreamResult(holdings);
            holdingsTransformer.transform(sourceHoldings,xsltResultHoldings);
            //System.out.println(holdings.toString());

            //after we fetched the holdings we are now going to "weed" the holdings
            //part of the holdings-information should be in the basic bibliographic record we are going to use
            //to present information in the result list
            //the complete holdings information is only needed for the full view of a single record
            Result xsltResultWeeding = new StreamResult(weededRecord);
            weedingsTransformer.transform(sourceWeeding,xsltResultWeeding);
            //System.out.println(weededRecord.toString());

            Source source = new StreamSource(new StringReader(weededRecord.toString()));

            try {
                for (int index = 0; index < transformers.size(); index++) {


                    if (index == transformers.size() - 1) {

                        //write it to fout in the last transformation step
                        Result tempXsltResult = new StreamResult(finalRecord);

                        //now store the complete holdings information as a parameter so it's easy for the template to fetch this information
                        //and use it for a SearchDoc Field
                        transformers.get(index).setParameter("holdingsStructure", holdings.toString());

                        //StringWriter test = new StringWriter();
                        //Result testResult  = new StreamResult(test);

                        transformers.get(index).transform(source, tempXsltResult);
                        //transformers.get(index).transform(source, testResult);
                        //System.out.println(test.toString());


                    } else {

                        //bei Zwischenschritten in der Transformation in einen String schreiben

                        StringWriter sw = new StringWriter();
                        Result tempXsltResult = new StreamResult(sw);
                        transformers.get(index).transform(source, tempXsltResult);

                        //if (isWriteIntermediateResult()) {
                        //    fileIntermediateResultOut.write(sw.toString() + ls);
                        //}
                        source = new StreamSource(new StringReader(sw.toString()));
                    }
                }
            } catch (Throwable thr) {
                thr.printStackTrace();
            }


            return finalRecord.toString();




        }

        @Override
        public void open(Configuration parameters) throws Exception {

            String allParams = parameters.getString("ALL_PROPERTIES","");
            String[] propKeys = allParams.split("###");

            Arrays.asList(propKeys).forEach((String key) ->
                    {
                        config.put(key.toUpperCase(), parameters.getString(key,""));

                    }
            );

            if (config.containsKey("PLUGINS.TO.LOAD".toUpperCase())){

                String pluginsConf = config.get("PLUGINS.TO.LOAD");
                ArrayList<String> pluginClassNames = new ArrayList<String>();
                pluginClassNames.addAll(Arrays.asList(pluginsConf.split("###")));


                for (String cName : pluginClassNames ) {
                    try {
                        Class tClass = Class.forName(cName);
                        IDocProcPlugin docProcPlugin = (IDocProcPlugin)tClass.newInstance();
                        docProcPlugin.initPlugin(config);
                        plugins.add(docProcPlugin);
                    } catch (ClassNotFoundException nfE) {
                        nfE.printStackTrace();
                    }

                }

            }


            System.setProperty("javax.xml.transform.TransformerFactory",getTransformerImplementation());

            transformerFactory = TransformerFactory.newInstance();

            try {
                for (Source source : getStreamSourcen()) {

                    transformers.add(transformerFactory.newTransformer(source));
                }
            } catch (Throwable thr) {
                thr.printStackTrace();
            }


            Source weedingsSource = getSearchedStreamSource(getxPathDirs(),config.get("WEEDHOLDINGS.TEMPLATE"));
            weedingsTransformer = transformerFactory.newTransformer(weedingsSource);
            Source holdingsSource = getSearchedStreamSource(getxPathDirs(),config.get("COLLECT.HOLDINGS.TEMPLATE"));
            holdingsTransformer = transformerFactory.newTransformer(holdingsSource);



            super.open(parameters);
        }

        private static String getTransformerImplementation() {

            String implementation = config.get("TRANSFORMERIMPL");
            //marcXMLlogger.info("\n => Loading TRANSFORMERIMPL: " + implementation);
            return null != implementation && implementation.length() > 0 ? implementation :"net.sf.saxon.TransformerFactoryImpl" ;
        }


        protected ArrayList<Source> getStreamSourcen() {
            String xsltTemplates = config.get("XSLTTEMPLATES");
            if (null == xsltTemplates || xsltTemplates.length() <= 0 ) {
                xsltTemplates = "swissbib.solr.xslt";
            }

            //marcXMLlogger.info("\n => Loading XSLTTEMPLATES: " + xsltTemplates);
            String[] templates = xsltTemplates.split("##");
            ArrayList<Source> sourcen  = new ArrayList<Source>();

            for (String template : templates) {

                //String p = configuration.get("XPATH.DIR") + fs +  template;
                sourcen.add(getSearchedStreamSource(getxPathDirs(),template) );
            }

            return sourcen;

        }

        protected StreamSource getSearchedStreamSource(ArrayList<String> directories, String filename) {

            StreamSource source = null;

            for (String baseDir : directories) {

                String path = baseDir + System.getProperties().getProperty("file.separator") +  filename;

                if (new File(path).exists())  {

                    source =   new StreamSource(path);
                    break;
                }
            }

            return source;

        }

        protected ArrayList<String> getxPathDirs () {


            if (this.xpathDirs == null) {

                String xPathes = config.get("XPATH.DIR");
                this.xpathDirs = new ArrayList<String>();
                xpathDirs.addAll(Arrays.asList(xPathes.split("###")));
            }

            return this.xpathDirs;
        }


    }


}
