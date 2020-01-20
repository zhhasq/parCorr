
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class TSCorrelation extends AbstractCustomOperator implements IJoinCallback, IWindowCallback, ICounterCallback {
    private AbstractStream pairStream = null;
    private AbstractStream tsStreamCorr;
    private AbstractStream candidateStream;
    private AbstractStream vocabularyStream;

    private FreeTuplePool freeTuplePool = FreeTuplePool.getFreeTuplePool();
    private static Logger log = Logger.getLogger(TSCorrelation.class);

    private int windowSize;
    private int windowSlide = 1;
    private int maxWindowLag = 0;
    private int threshold;
    private boolean candOnly = false;
    private String targets;
    private String features;

    private Object lastSystemTimestamp;
    private Object lastUserTimestamp;

    private HashMap< String, Counter > counterMap = new HashMap<>();
    private LinkedList<String> eventVocabulary = new LinkedList<>();

    private Storage storage = new Storage(this);
    private WindowControl windowControl = new WindowControl(this);

    private int eventsOut = 0;

    public TSCorrelation(Long subQueryInstanceID, String operatorName, java.util.ArrayList<AbstractStream> inStreams, ArrayList<AbstractStream> outStreams, AbstractStream timeoutStream) {
        super(subQueryInstanceID, operatorName, inStreams, outStreams, timeoutStream);

        log.info("INRIA: #" + this.hashCode() + " created");

        for (AbstractStream s : inStreams) {
            if (s.getStreamName().equals("pairStream"))
                this.pairStream = s;
            else if (s.getStreamName().equals("tsStreamCorr"))
                this.tsStreamCorr = s;
            else if (s.getStreamName().equals("vocabularyStream"))
                this.vocabularyStream = s;
            s.registerOperator(this);
        }

        for (AbstractStream s : outStreams) {
            if (s.getStreamName().equals("candidateStream") || s.getStreamName().equals("outStream"))
                this.candidateStream = s;
        }
    }

    private void emitCandidate( String eventId, String pairEventId, byte[] pairTimeSeries, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException {
        ++eventsOut;

        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("EVENT", Utils.getBaseEvent(eventId));
        t.addField("EVENTID", eventId);
        t.addField("PAIREVENT", pairEventId);
        t.addField("PAIRVECTOR", pairTimeSeries);

        emit(t);
    }

    private void emitCandidate( String eventId, String pairEventId, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException {
        ++eventsOut;

        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("EVENT1", eventId);
        t.addField("EVENT2", pairEventId);
        t.addField("CORR", System.currentTimeMillis() + 0.0);

        emit(t);
    }

    private void handleLinearSearch( String eventId, TimeSeries ts, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException {

        for ( String pairEventId : eventVocabulary ) {

            StringBuffer featuresBuff = new StringBuffer(features);
            if ( (targets.length() == 0 && pairEventId.compareTo(eventId) < 0) || ( targets.length() > 0 && pairEventId.compareTo(eventId) != 0 && Utils.matchesTarget( pairEventId, targets, featuresBuff ) ) ) {

                int maxLag = (int) Math.min((long)maxWindowLag, ts.getElemCount() - (long)windowSize );
                for ( int lag = 0 ; lag <= maxLag ; ++lag ) {
                    String compositeEventId = eventId + (lag > 0 ? ("@-" + String.valueOf(lag)) : "");
                    if ( !compositeEventId.equals(pairEventId)
                            && ( targets.length() == 0 || features.length() == 0 || compositeEventId.matches(featuresBuff.toString()) ) )
                    {
                        double[] data = ts.arrayWithStats(lag);
                        emitCandidate(pairEventId, compositeEventId, Utils.doublesToBytes(data), systemTimestamp, userTimestamp);
                    }
                }

            }
        }
    }

    private void processData(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        this.lastSystemTimestamp = tuple.getField("cep_system_timestamp");
        this.lastUserTimestamp = tuple.getField("cep_user_timestamp");
        long userTS = (Long) this.lastUserTimestamp;

        if ( !windowControl.handleEvent( userTS, true ) )
            return;


        String eventId = (String) tuple.getField("EVENTID");
        String pairEvents = (String) tuple.getField("PAIREVENT");

        Counter c = this.counterMap.get(eventId);
        if (c == null)
            this.counterMap.put(eventId, Counter.deserialize(pairEvents, this, eventId, threshold));
        else
            c.merge(Counter.deserialize(pairEvents));
    }

    private void processVector(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        long userTS = (Long) tuple.getField("cep_user_timestamp");
        String eventId = (String) tuple.getField("EVENT");
        byte[] ts_data = (byte[]) tuple.getField("VECTOR");

        TimeSeries ts = this.storage.putTS( userTS, eventId, Utils.bytesToDoubles(ts_data) );

        if ( !eventVocabulary.isEmpty() )
            handleLinearSearch( eventId, ts, tuple.getField("cep_system_timestamp"), userTS);
    }

    private void processVocabulary(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        String eventId = (String) tuple.getField("EVENT");

        if (eventId.startsWith("|"))
            return;

        for (String e : eventId.split("\\|"))
            eventVocabulary.add(e);

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
                case "threshold": this.threshold = Integer.valueOf(c.getConfigValue()); break;
                case "candOnly": this.candOnly = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "targets": this.targets = c.getConfigValue(); if (targets.equals("|")) targets = ""; break;
                case "features": this.features = c.getConfigValue(); if (features.equals("|")) features = ""; break;
                default: log.warn("unknown parameter: " + c.getConfigKey()); break;
            }
        }

        this.storage.setConfig(windowSize, maxWindowLag + 1 + 10 * windowSlide);
    }

    @Override
    protected void process(String s, Tuple tuple) throws CepTupleException, CepOperatorException {
        if ( this.pairStream != null && s.equals(this.pairStream.getStreamName()) )
            processData(tuple);
        else if ( s.equals(this.tsStreamCorr.getStreamName()) )
            processVector(tuple);
        else if ( s.equals(this.vocabularyStream.getStreamName()) )
            processVocabulary(tuple);
    }

    @Override
    public void emit(Tuple tuple) throws CepOperatorException {
        try {
            this.candidateStream.send(tuple);
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
    public void handleJoinTuple( long timestamp, long index, String key, Object lhs, Object rhs ) throws CepOperatorException {
        String eventId = (String) rhs;
        double[] pairTimeSeries = (double[]) lhs;

        emitCandidate( eventId, key, Utils.doublesToBytes(pairTimeSeries), this.lastSystemTimestamp, timestamp );
    }

    @Override
    public void handleTimestampAdvance(long oldTS, long newTS, int eventsProcessed, int eventsDiscarded, long timeMs) throws CepOperatorException {

        log.info("INRIA: #" + System.currentTimeMillis() + "; Window slide(" + oldTS + "); Events: " + eventsProcessed + " processed, " + eventsDiscarded + " discarded, " + eventsOut + " emitted in " + timeMs + "ms");

        this.counterMap.clear();
        eventsOut = 0;
    }

    @Override
    public void handleCounterThreshold(String key, String value) throws CepOperatorException {
        if (candOnly)
            this.emitCandidate(key, value, this.lastSystemTimestamp, this.lastUserTimestamp);
        else
            this.storage.queryTS( (Long) this.lastUserTimestamp, key, value );

    }
}
