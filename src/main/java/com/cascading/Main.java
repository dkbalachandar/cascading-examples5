
package com.cascading;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * A Cascading example to read a text file which contains user name and age details and remove the users whose age is more than or equals to 30
 * and also print the content in an output file with some predefined expression
 */
public class Main {


    /**
     * This examples uses ExpressionFilter and  ExpressionFunction function
     *
     * @param args
     */
    public static void main(String[] args) {

        System.out.println("Hdfs Job Starts");

        //input and output path
        String inputPath = args[0];
        String outputPath = args[1];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);

        //Create the source tap
        Fields fields = new Fields("userName", "age");
        Tap inTap = new Hfs(new TextDelimited(fields, true, "\t"), inputPath);

        //Create the sink tap
        Tap outTap = new Hfs(new TextDelimited(false, "\t"), outputPath, SinkMode.REPLACE);

        // Pipe to connect Source and Sink Tap
        Pipe dataPipe = new Pipe("data");
        //Adding the expression filter, if the expression returns true, then that tuple will be removed
        ExpressionFilter filter = new ExpressionFilter("age >= 30", Integer.TYPE);
        dataPipe = new Each(dataPipe, new Fields("userName", "age"), filter);
        //Finally we use the expression function to add some predefined sentences
        String expression = "\"The user name is \" + userName + \" and his age is \" + age";
        ExpressionFunction function = new ExpressionFunction(new Fields(" "), expression, String.class);
        dataPipe = new Each(dataPipe, new Fields("userName", "age"), function);

        //Add the FlowDef and call the process
        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);
        FlowDef flowDef = FlowDef.flowDef().addSource(dataPipe, inTap).addTailSink(dataPipe, outTap).setName("Hdfs Job");
        flowConnector.connect(flowDef).complete();
        System.out.println("Hdfs Job Ends");
    }
}

