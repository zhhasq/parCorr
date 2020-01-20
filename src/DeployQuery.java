
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

import es.upm.cep.client.CEPDriverManager;
import es.upm.cep.client.OrchestratorConnection;
import es.upm.cep.client.factory.AutomaticQueryFactory;
import es.upm.cep.client.factory.CustomOperatorConfig;
import es.upm.cep.commons.exception.CepException;
import es.upm.cep.commons.exception.CepQueryFactoryException;
import es.upm.cep.commons.json.driver.DeployQueryJSON;
import es.upm.cep.commons.json.driver.RegisterQueryJSON;
import es.upm.cep.commons.json.driver.ResponseStatusJSON;
import es.upm.cep.commons.type.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Random;

public class DeployQuery {
    AutomaticQueryFactory query;

    private String queryName = "QueryTS";
    private int parallelism = 0;

    private int windowSize = 500;
    private int windowSlide = 20;
    private int timeUnit = 1;
    private int maxWindowLag = 0;
    private int hyperWindow = 0;
    private int sketchSize = 30;
    private int gridDimensions = 2;
    private int threshold = 6;
    private int eventBuffer = 0;
    private int maxSamples = 0;
    private int maxFeatures = 10;
    private int gridSize = 0;
    private int sampleSize = 0;
    private int vocabularyBuffer = 399;
    private double cellSize = 10;
    private double minCorr = 0.9;
    private boolean searchInverse = false;
    private boolean candOnly = false;
    private boolean linearSearch = false;
    private boolean varcovOutput = false;
    private boolean skipTraining = false;
    private boolean pipeline = false; // use this only when features cannot be targets
    private String targets = "|";
    private String features = "|";
    private String rmfile = "";
    private String bpfile = "";
    private String inputFile = null;
    private String useCase = null;
    private String duplAgg = "LAST";

    class SocketAddr {
        SocketAddr (String ipPort)
        {
            String[] tokens = ipPort.split(":");
            ip = tokens[0];
            port = Integer.valueOf(tokens[1]);
        }

        String ip;
        int port;
    }

    private SocketAddr inputSocket = null;
    private SocketAddr outputSocket = null;
    private SocketAddr targetSocket = null;

    private int instances() {
        if (parallelism == 0)
            return 1;
        return 10; // parallelism * ((candOnly ? 3 : 4) + (useCase == null ? 0 : 1));
    }

    private void setConfig(String param, String value)
    {
        switch (param) {
            case "queryName": queryName = value; break;
            case "parallelism": parallelism = Integer.valueOf(value); break;
            case "windowSize": windowSize = Integer.valueOf(value); break;
            case "windowSlide": windowSlide = Integer.valueOf(value); break;
            case "timeUnit": timeUnit = Integer.valueOf(value); break;
            case "maxWindowLag": maxWindowLag = Integer.valueOf(value); break;
            case "hyperWindow": hyperWindow = Integer.valueOf(value); break;
            case "sketchSize": sketchSize = Integer.valueOf(value); break;
            case "gridDimensions": gridDimensions = Integer.valueOf(value); break;
            case "threshold": threshold = Integer.valueOf(value); break;
            case "eventBuffer": eventBuffer = Integer.valueOf(value); break;
            case "maxSamples": maxSamples = Integer.valueOf(value); break;
            case "maxFeatures": maxFeatures = Integer.valueOf(value); break;
            case "gridSize": gridSize = Integer.valueOf(value); break;
            case "sampleSize": sampleSize = Integer.valueOf(value); break;
            case "vocabularyBuffer": vocabularyBuffer = Integer.valueOf(value); break;
            case "cellSize": cellSize = Double.valueOf(value); break;
            case "minCorr": minCorr = Double.valueOf(value); break;
            case "searchInverse": searchInverse = value.equalsIgnoreCase("true"); break;
            case "candOnly": candOnly = value.equalsIgnoreCase("true"); break;
            case "linearSearch": linearSearch = value.equalsIgnoreCase("true"); break;
            case "varcovOutput": varcovOutput = value.equalsIgnoreCase("true"); break;
            case "skipTraining": skipTraining = value.equalsIgnoreCase("true"); break;
            case "pipeline": pipeline = value.equalsIgnoreCase("true"); break;
            case "targets": targets = value; break;
            case "features": features = value; break;
            case "rmfile": rmfile = value; break;
            case "bpfile": bpfile = value; break;
            case "inputFile": inputFile = value; break;
            case "useCase": useCase = value; break;
            case "duplAgg": duplAgg = value; break;
            case "inputSocket": inputSocket = new SocketAddr(value); break;
            case "outputSocket": outputSocket = new SocketAddr(value); break;
            case "targetSocket": targetSocket = new SocketAddr(value); break;
        }
    }

    private void readConfig(String filename)
    {
        List<String> lines;
        try {
            lines = Files.readAllLines(Paths.get(filename));
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        for (String l : lines) {
            if ( l.length() > 0 && l.charAt(0) != ';') {
                String[] p = l.split("=");
                if (p.length == 2)
                    setConfig(p[0].trim(), p[1].trim());
            }
        }
    }

    private String readRandomMatrix(String filename)
    {
        try {
            return new String(Files.readAllBytes(Paths.get(filename)));
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        }
    }

    private String generateRandomMatrix()
    {
        String ret = "";
        Random rand = new Random();
        for ( int i = 0 ; i < windowSize*sketchSize ; ++i )
            ret = ret.concat( Integer.toString(rand.nextInt(2)) );
        return ret;
    }

    public void deploy() {
        try {
            OrchestratorConnection connection;

            connection = CEPDriverManager.getConnection("deployer");

            {
                try {
                    connection.unDeployQuery(queryName);
                    connection.unRegisterQuery(queryName);
                } catch (CepException e) {
                    System.out.println( "Couldn't undeploy: " + e.getMessage());
                }

                ResponseStatusJSON registerQuery = connection.registerQuery(this.buildQueryDescriptor());
                System.out.println(registerQuery.getResponseDescription());

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                ResponseStatusJSON deployQuery = connection.deployQuery(this.buildQueryDeploymentDescriptor());
                System.out.println(deployQuery.getResponseDescription());
            }

            connection.disconnect();
        } catch (CepException e) {
            System.out.println("deploy error: " + e.getMessage());
            System.exit(1);
        }

    }

    private RegisterQueryJSON buildQueryDescriptor() {
        RegisterQueryJSON queryJSON = null;
        query = new AutomaticQueryFactory(queryName, instances());

        try {
            if (useCase == null) {
                Schema in = new Schema();
                in.addLongField("timestamp");
                in.addTextField("EVENT");
                in.addDoubleField("VALUE");

                if (inputSocket == null)
                    query.addStream("inStream", in);
                else
                    query.addStream("inStream", in, inputSocket.ip, inputSocket.port);

            } else if (useCase.equals("UC1"))
                PriceAggQuery.uc1PriceQuery(query, inputFile, inputSocket.ip, inputSocket.port, 1);

            Schema vectorStream = new Schema();
            vectorStream.addTextField("EVENT");
            vectorStream.addByteField("VECTOR");

            Schema sketchStream = new Schema();
            sketchStream.addTextField("EVENTID");
            sketchStream.addTextField("GRIDCELL");

            Schema pairStream = new Schema();
            pairStream.addTextField("EVENT");
            pairStream.addTextField("EVENTID");
            pairStream.addTextField("PAIREVENT");

            Schema candidateStream = new Schema();
            candidateStream.addTextField("EVENT");
            candidateStream.addTextField("EVENTID");
            candidateStream.addTextField("PAIREVENT");
            candidateStream.addByteField("PAIRVECTOR");

            Schema outStream = new Schema();
            outStream.addTextField("EVENT1");
            outStream.addTextField("EVENT2");
            outStream.addDoubleField("CORR");

            Schema targetStream = new Schema();
            targetStream.addTextField("TARGET");
            targetStream.addTextField("PREDICTORS");
            targetStream.addTextField("COEFS");
//            targetStream.addTextField("EVENTS");
//            targetStream.addByteField("DATA");

            Schema vocabulary = new Schema();
            vocabulary.addTextField("EVENT");


            {
                CustomOperatorConfig sketcher = new CustomOperatorConfig("sketcher", "inria.TSSketcher", new String[]{"inStream"}, true);

                if (!linearSearch)
                    sketcher.addOutputStreamSchema("sketchStream", sketchStream);
                sketcher.addOutputStreamSchema("tsStreamCorr", vectorStream);
                sketcher.addOutputStreamSchema("tsStreamVerif", vectorStream);
                sketcher.addOutputStreamSchema("vocabularyStream", vocabulary);
                sketcher.setUserTimestampConfig("inStream.cep_user_timestamp");
                sketcher.addConfiguration("windowSize", Integer.toString(windowSize));
                sketcher.addConfiguration("windowSlide", Integer.toString(windowSlide));
                sketcher.addConfiguration("timeUnit", Integer.toString(timeUnit));
                sketcher.addConfiguration("maxWindowLag", Integer.toString(maxWindowLag));
                sketcher.addConfiguration("sketchSize", Integer.toString(sketchSize));
                sketcher.addConfiguration("gridDimensions", Integer.toString(gridDimensions));
                sketcher.addConfiguration("gridSize", Integer.toString(gridSize));
                sketcher.addConfiguration("sampleSize", Integer.toString(sampleSize));
                sketcher.addConfiguration("vocabularyBuffer", Integer.toString(vocabularyBuffer));
                sketcher.addConfiguration("cellSize", Double.toString(cellSize));
                sketcher.addConfiguration("searchInverse", searchInverse ? "true" : "false");
                sketcher.addConfiguration("candOnly", candOnly ? "true" : "false");
                sketcher.addConfiguration("linearSearch", linearSearch ? "true" : "false");
                sketcher.addConfiguration("randomMatrix", rmfile.length() > 0 ? readRandomMatrix(rmfile) : generateRandomMatrix());
                sketcher.addConfiguration("bpstr", bpfile.length() > 0 ? readRandomMatrix(bpfile) : "|");
                sketcher.addConfiguration("targets", targets);
                sketcher.addConfiguration("duplAgg", duplAgg);
                query.addOperator(sketcher);
            }

            if (!linearSearch) {
                CustomOperatorConfig colloc = new CustomOperatorConfig("colloc", "inria.TSCollocation", new String[]{"sketchStream"}, true);

                colloc.addOutputStreamSchema("pairStream", pairStream);
                colloc.addConfiguration("eventBuffer", Integer.toString(eventBuffer));
                colloc.addConfiguration("searchInverse", searchInverse ? "true" : "false");
                colloc.addConfiguration("pipeline", pipeline ? "true" : "false");
                colloc.addConfiguration("targets", targets);
                colloc.addConfiguration("features", features);
                colloc.setUserTimestampConfig("sketchStream.cep_user_timestamp");
                query.addOperator(colloc);
            }

            {
                String[] inStreams = linearSearch ?
                        new String[]{"tsStreamCorr", "vocabularyStream"} :
                        new String[]{"pairStream", "tsStreamCorr", "vocabularyStream"} ;

                CustomOperatorConfig candidates = new CustomOperatorConfig("candidates", "inria.TSCorrelation", inStreams, true);
                candidates.addBroadcastStream("vocabularyStream");

                if (candOnly)
                    candidates.addOutputStreamSchema("outStream", outStream);
                else
                    candidates.addOutputStreamSchema("candidateStream", candidateStream);
                candidates.setUserTimestampConfig("pairStream.cep_user_timestamp");
                candidates.addConfiguration("windowSize", Integer.toString(windowSize));
                candidates.addConfiguration("windowSlide", Integer.toString(windowSlide));
                candidates.addConfiguration("maxWindowLag", Integer.toString(maxWindowLag));
                candidates.addConfiguration("threshold", Integer.toString(threshold));
                candidates.addConfiguration("candOnly", candOnly ? "true" : "false");
                candidates.addConfiguration("targets", targets);
                candidates.addConfiguration("features", features);
                query.addOperator(candidates);
            }

            if ( !candOnly ) {
                CustomOperatorConfig verification = new CustomOperatorConfig("verification", "inria.TSVerification", new String[]{"candidateStream", "tsStreamVerif"}, true);
                verification.addOutputStreamSchema("outStream", outStream);
                if (!targets.equals("|"))
                    verification.addOutputStreamSchema("targetStream", targetStream);
                verification.setUserTimestampConfig("candidateStream.cep_user_timestamp");
                verification.addConfiguration("windowSize", Integer.toString(windowSize));
                verification.addConfiguration("windowSlide", Integer.toString(windowSlide));
                verification.addConfiguration("maxWindowLag", Integer.toString(maxWindowLag));
                verification.addConfiguration("hyperWindow", Integer.toString(hyperWindow));
                verification.addConfiguration("maxSamples", Integer.toString(maxSamples));
                verification.addConfiguration("maxFeatures", Integer.toString(maxFeatures));
                verification.addConfiguration("minCorr", Double.toString(minCorr));
                verification.addConfiguration("searchInverse", searchInverse ? "true" : "false");
                verification.addConfiguration("varcovOutput", varcovOutput ? "true" : "false");
                verification.addConfiguration("skipTraining", skipTraining ? "true" : "false");
                query.addOperator(verification);
            }

            if (outputSocket != null)
                query.registerDataSink("outStream", outputSocket.ip, outputSocket.port);

            if (!targets.equals("|") && targetSocket != null)
                query.registerDataSink("targetStream", targetSocket.ip, targetSocket.port);

            queryJSON = query.getQueryDescriptor();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

        return queryJSON;
    }

    private DeployQueryJSON buildQueryDeploymentDescriptor() {

        DeployQueryJSON queryToDeploy = null;

        try {
            queryToDeploy = query.getQueryDeploymentDescriptor();
        } catch (CepQueryFactoryException e) {
            System.out.println(e.getMessage());
            System.exit(1);
        }

        if ( parallelism > 1 ) {
            int sqs = queryToDeploy.getSubQueryDeploymentJSONArray().length;

            System.out.println( "instances=" + sqs * parallelism );

            for ( int i = 0 ; i < sqs ; ++i ) {
                queryToDeploy.getSubQueryDeploymentJSON("SQ_" + queryName + "_" + String.valueOf(i)).setNumberOfInstances(parallelism);
            }

            if (!candOnly)
                queryToDeploy.getSubQueryDeploymentJSON("SQ_" + queryName + "_" + String.valueOf(--sqs)).setRouteKey("EVENT");
            queryToDeploy.getSubQueryDeploymentJSON("SQ_" + queryName + "_" + String.valueOf(--sqs)).setRouteKey("EVENT");
            if (!linearSearch)
                queryToDeploy.getSubQueryDeploymentJSON("SQ_" + queryName + "_" + String.valueOf(--sqs)).setRouteKey("GRIDCELL");
            queryToDeploy.getSubQueryDeploymentJSON("SQ_" + queryName + "_" + String.valueOf(--sqs)).setRouteKey("EVENT");

        }

        return queryToDeploy;
    }

    public static void main(String[] args) {
        DeployQuery m = new DeployQuery();
        m.readConfig(args[0]);
        m.deploy();
    }
}
