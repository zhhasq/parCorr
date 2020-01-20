
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import es.upm.cep.commons.exception.*;
import es.upm.cep.commons.json.ConfigDefinition;
import es.upm.cep.commons.leancollection.FreeTuplePool;
import es.upm.cep.core.operator.custom.AbstractCustomOperator;
import es.upm.cep.core.stream.AbstractStream;
import es.upm.cep.commons.type.Tuple;
import org.apache.log4j.Logger;

import java.util.*;

public class TSVerification extends AbstractCustomOperator implements IJoinCallback, IWindowCallback {
    private AbstractStream candidateStream;
    private AbstractStream tsStreamVerif;
    private AbstractStream outStream;
    private AbstractStream targetStream = null;

    private FreeTuplePool freeTuplePool = FreeTuplePool.getFreeTuplePool();
    private static Logger log = Logger.getLogger(TSVerification.class);

    private int windowSize;
    private int windowSlide = 1;
    private int maxWindowLag = 0;
    private int hyperWindow = 0;

    private int maxSamples = 0;
    private int maxFeatures = 10;
    private double minCorr;
    private boolean searchInverse = false;
    private boolean varcovOutput = false;
    private boolean skipTraining = false;

    private Object lastSystemTimestamp;

    private long firstOutTime = 0;
    private long lastOutTime = 0;

    private Storage storage = new Storage(this);
    private WindowControl windowControl = new WindowControl(this);

    private CorrCache corrCache = null;
    private Regression regression = null;

    private int eventsOut = 0;
    private HashSet<String> processedPairs = new HashSet<>();

    public TSVerification(Long subQueryInstanceID, String operatorName, java.util.ArrayList<AbstractStream> inStreams, ArrayList<AbstractStream> outStreams, AbstractStream timeoutStream)
    {
        super(subQueryInstanceID, operatorName, inStreams, outStreams, timeoutStream);

        log.info("INRIA: #" + this.hashCode() + " created");

        for (AbstractStream s : inStreams) {
            if (s.getStreamName().equals("candidateStream"))
                this.candidateStream = s;
            else if (s.getStreamName().equals("tsStreamVerif"))
                this.tsStreamVerif = s;
            s.registerOperator(this);
        }

        for (AbstractStream s : outStreams) {
            if (s.getStreamName().equals("outStream"))
                this.outStream = s;
            else if (s.getStreamName().equals("targetStream"))
                this.targetStream = s;
        }
    }

    private void emitPair( String event1, String event2, Double corr, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        ++eventsOut;

        long currTS = System.currentTimeMillis();
        if (firstOutTime == 0)
            firstOutTime = currTS;
        lastOutTime = currTS;

        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("EVENT1", event1);
        t.addField("EVENT2", event2);
        t.addField("CORR", corr);

        emit(t);
    }

    private void processData(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        long userTS = (Long) tuple.getField("cep_user_timestamp");
        this.lastSystemTimestamp = tuple.getField("cep_system_timestamp");

        if ( !windowControl.handleEvent( userTS, true ) )
            return;

        String eventId = (String) tuple.getField("EVENTID");
        this.storage.queryTS( userTS, eventId, tuple );
    }

    private void processVector(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        long userTS = (Long) tuple.getField("cep_user_timestamp");
        String eventId = (String) tuple.getField("EVENT");
        double[] ts = Utils.bytesToDoubles( (byte[]) tuple.getField("VECTOR") );

        this.storage.putTS( userTS, eventId, ts );

        if ( this.varcovOutput )
            emitPair(eventId, eventId, ts[ts.length-1] * ts[ts.length-1], tuple.getField("cep_system_timestamp"), userTS);
    }

    @Override
    public void printStats(long timeinterval) {
        this.stats.processOpMetrics(timeinterval);
    }

    @Override
    public void setConfig(ConfigDefinition[] configDefinitions) throws BadFormatException, NullParameterException {
        for (ConfigDefinition c : configDefinitions) {
            switch (c.getConfigKey()) {
                case "windowSize": this.windowSize = Integer.valueOf(c.getConfigValue()); break;
                case "windowSlide": this.windowSlide = Integer.valueOf(c.getConfigValue()); break;
                case "maxWindowLag": this.maxWindowLag = Integer.valueOf(c.getConfigValue()); break;
                case "hyperWindow": this.hyperWindow = Integer.valueOf(c.getConfigValue()); break;
                case "maxSamples": this.maxSamples = Integer.valueOf(c.getConfigValue()); break;
                case "maxFeatures": this.maxFeatures = Integer.valueOf(c.getConfigValue()); break;
                case "minCorr": this.minCorr = Double.valueOf(c.getConfigValue()); break;
                case "searchInverse": this.searchInverse = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "varcovOutput": this.varcovOutput = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "skipTraining": this.skipTraining = (c.getConfigValue().equalsIgnoreCase("true")); break;
                default: log.warn("unknown parameter: " + c.getConfigKey()); break;
            }
        }

        this.storage.setConfig(windowSize, maxWindowLag + 1 + 10 * windowSlide);
        this.corrCache = new CorrCache(hyperWindow < windowSize ? windowSize : hyperWindow, maxWindowLag + 1 + 10 * windowSlide);

        if (targetStream != null)
            this.regression = new Regression(targetStream, windowSize, maxSamples, maxFeatures, skipTraining);
    }

    @Override
    protected void process(String s, Tuple tuple) throws CepTupleException, CepOperatorException {
        if ( s.equals(this.candidateStream.getStreamName()) )
            processData(tuple);
        else if ( s.equals(this.tsStreamVerif.getStreamName()) )
            processVector(tuple);
    }

    @Override
    public void emit(Tuple tuple) throws CepOperatorException {
        try {
            this.outStream.send(tuple);
        }
        catch (CepStreamException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isStateful() {
        return true;
    }

    @Override
    public String[] getRouteFields() {
        return new String[] {"EVENT"};
    }

    @Override
    public void handleJoinTuple(long timestamp, long index, String key, Object lhs, Object rhs) throws CepOperatorException
    {
        Tuple tuple = (Tuple) rhs;
        Object sysTS = tuple.getField("cep_system_timestamp");
        String eventId = key;
        String pairEventId = (String) tuple.getField("PAIREVENT");
        byte[] pairTimeSeries = (byte[]) tuple.getField("PAIRVECTOR");

        double[] ts = (double[]) lhs;
        double[] pair_ts = Utils.bytesToDoubles( pairTimeSeries );

        Double Corr = corrCache.process(index, eventId, ts, pairEventId, pair_ts);
        if (Corr == null)
            Corr = Utils.corrWithStats( ts, pair_ts );

        double corr = Corr == null ? 0 : Corr;

        if (eventId.endsWith("-")) {
            eventId = eventId.substring(0, eventId.length()-1);
            corr = -corr;
        }
        if (pairEventId.endsWith("-")) {
            pairEventId = pairEventId.substring(0, pairEventId.length()-1);
            corr = -corr;
        }

        if ( !processedPairs.add(eventId + "|" + pairEventId) )
            return;

        if (regression != null && maxFeatures > 0 && Corr != null)
            regression.addTargetEvent(eventId, pairEventId, corr, Utils.doublesToBytes(ts), pairTimeSeries);

        if ( corr >= minCorr || (searchInverse && corr <= -minCorr) ) {

            if ( this.varcovOutput )
                emitPair(eventId, pairEventId, corr * ts[ts.length-1] * pair_ts[pair_ts.length-1], sysTS, timestamp);
            else
                emitPair(eventId, pairEventId, corr, sysTS, timestamp);

            if (regression != null && maxFeatures == 0)
                regression.addTargetEvent(eventId, pairEventId, corr, Utils.doublesToBytes(ts), pairTimeSeries);
        }


    }

    @Override
    public void handleTimestampAdvance(long oldTS, long newTS, int eventsProcessed, int eventsDiscarded, long timeMs) throws CepOperatorException {

        log.info("INRIA: #" + System.currentTimeMillis() + "; Window slide(" + oldTS + "); Events: " + eventsProcessed + " processed, " + eventsDiscarded + " discarded, " + eventsOut + " emitted in " + timeMs + "ms" + "; first out " + firstOutTime + ", last out " + lastOutTime);

        firstOutTime = 0;
        eventsOut = 0;
        processedPairs.clear();

        if (regression != null)
            regression.flushTargets((Long)lastSystemTimestamp, oldTS);
    }
}
