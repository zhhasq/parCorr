
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

public class TSCollocation extends AbstractCustomOperator implements IWindowCallback {

    private AbstractStream sketchStream;
    private AbstractStream pairStream;

    private FreeTuplePool freeTuplePool = FreeTuplePool.getFreeTuplePool();
    private static Logger log = Logger.getLogger(TSCollocation.class);

    private int bufferSize;
    private boolean searchInverse = false;
    private boolean pipeline = false;
    private String targets;
    private String features;

    private int eventsBuffered = 0;

    private Object lastSystemTimestamp;

    private HashMap< String, LinkedList<String> > cellMap = new HashMap<>();
    private HashMap< String, Counter > counterMap = new HashMap<>();

    private HashMap< String, Counter > cellsWithTargets = new HashMap<>();
    private HashMap< String, String > featuresRegex = new HashMap<>();

    private WindowControl windowControl = new WindowControl(this);

    public TSCollocation(Long subQueryInstanceID, String operatorName, java.util.ArrayList<AbstractStream> inStreams, ArrayList<AbstractStream> outStreams, AbstractStream timeoutStream)
    {
        super(subQueryInstanceID, operatorName, inStreams, outStreams, timeoutStream);

        log.info("INRIA: #" + this.hashCode() + " created");

        for (AbstractStream s : inStreams) {
            if (s.getStreamName().equals("sketchStream"))
                this.sketchStream = s;
            s.registerOperator(this);
        }

        for (AbstractStream s : outStreams) {
            if (s.getStreamName().equals("pairStream"))
                this.pairStream = s;
        }
    }

    private void handleEntry( String cellId, String eventId, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        LinkedList<String> eventIds = this.cellMap.get(cellId);

        if ( eventIds == null ) {
            eventIds = new LinkedList<>();
            eventIds.add(eventId);
            this.cellMap.put(cellId, eventIds);

        } else {
            boolean isInserted = false;

            ListIterator<String> it = eventIds.listIterator();
            while ( it.hasNext() ) {
                String pairEventId = it.next();

                if ( isInserted )
                    handlePair( eventId, pairEventId, systemTimestamp, userTimestamp );

                else if ( eventId.compareTo( pairEventId ) > 0 )
                    handlePair( pairEventId, eventId, systemTimestamp, userTimestamp );

                else {
                    it.previous();
                    it.add(eventId);
                    it.next();
                    isInserted = true;

                    handlePair( eventId, pairEventId, systemTimestamp, userTimestamp );
                }

            }
            if (!isInserted)
                eventIds.add(eventId);
        }
    }

    private void handleEntryForTargets( String cellId, String eventId, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        if (!pipeline) {
            LinkedList<String> eventIds = this.cellMap.computeIfAbsent(cellId, k -> new LinkedList<>());
            eventIds.add(eventId);
        }

        Counter c = this.cellsWithTargets.get(cellId);

        if (pipeline && c != null) {
            String pairEvents = c.serialize( target -> !Utils.getBaseEvent(eventId).equals(target) &&  eventId.matches(featuresRegex.get(target)) );

            if ( pairEvents.length() > 0 )
                emitPair( eventId, pairEvents, systemTimestamp, userTimestamp );
        }

        StringBuffer featuresBuff = new StringBuffer(features);

        if ( Utils.matchesTarget( eventId, targets, featuresBuff) ) {
            this.featuresRegex.computeIfAbsent(eventId, k -> featuresBuff.toString());

            if (c != null)
                c.add(eventId);
            else
                this.cellsWithTargets.put(cellId, new Counter(eventId));

        }
    }

    private boolean shallHandle(String eventId) {
        return !(searchInverse && eventId.endsWith("-"));
    }

    private void handlePair( String event1, String event2, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {   // TODO: deal with targets
        if (bufferSize == 0)
            return;

        if ( !shallHandle(event1) )
            return;

        updateCounter(event1, event2);

        if ( ++eventsBuffered >= bufferSize )
            flushCounters( systemTimestamp, userTimestamp );
    }

    private void updateCounter(String event1, String event2) throws CepOperatorException
    {
        Counter c = this.counterMap.get(event1);
        if ( c != null )
            c.add(event2);
        else
            this.counterMap.put( event1, new Counter(event2) );
    }

    private void flushCounters( Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        for ( HashMap.Entry<String, Counter> entry : this.counterMap.entrySet() ) {
            String eventId = entry.getKey();
            String pairEvents = entry.getValue().serialize();

            emitPair( eventId, pairEvents, systemTimestamp, userTimestamp );
        }

        this.counterMap.clear();
        this.eventsBuffered = 0;
    }

    private void flushCells() throws CepOperatorException
    {
        for ( HashMap.Entry<String, LinkedList<String>> entry : this.cellMap.entrySet() ) {

            Iterator<String> it = entry.getValue().descendingIterator();
            Counter c = new Counter(it.next());

            while (it.hasNext()) {
                String eventId = it.next();

                if ( shallHandle(eventId) ) {
                    Counter pairEvents = this.counterMap.get(eventId);
                    if (pairEvents == null) {
                        Counter cp = new Counter();
                        cp.merge(c);
                        this.counterMap.put(eventId, cp);
                    } else {
                        pairEvents.merge(c);
                    }
                }

                c.add(eventId);
            }
        }
    }

    private void flushCellsWithTargets( Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        for ( HashMap.Entry<String, Counter> entry : this.cellsWithTargets.entrySet() ) {
            String cellId = entry.getKey();
            Counter c = entry.getValue();
            List<String> list = this.cellMap.get(cellId);

            for ( String featureId : list ) {
                String pairEvents = c.serialize( target -> !Utils.getBaseEvent(featureId).equals(target) && featureId.matches(featuresRegex.get(target)) );

                if ( pairEvents.length() > 0 )
                    emitPair(featureId, pairEvents, systemTimestamp, userTimestamp);
            }
        }
    }

    private void emitPair( String event1, String event2, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("EVENT", Utils.getBaseEvent(event1));
        t.addField("EVENTID", event1);
        t.addField("PAIREVENT", event2);
        emit(t);
    }

    private void processData(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        this.lastSystemTimestamp = tuple.getField("cep_system_timestamp");
        long userTS = (Long) tuple.getField("cep_user_timestamp");

        if ( !windowControl.handleEvent( userTS, true ) )
            return;

        Object sysTS = tuple.getField("cep_system_timestamp");
        String eventId = (String) tuple.getField("EVENTID");
        String cellId = (String) tuple.getField("GRIDCELL");

        if ( targets.length() == 0 )
            handleEntry(cellId, eventId, sysTS, userTS);
        else
            handleEntryForTargets(cellId, eventId, sysTS, userTS);
    }

    @Override
    public void printStats(long timeinterval) {
        this.stats.processOpMetrics(timeinterval);
    }

    @Override
    public void setConfig(ConfigDefinition[] configDefinitions) throws BadFormatException, NullParameterException
    {
        for (ConfigDefinition c : configDefinitions) {
            switch (c.getConfigKey()) {
                case "eventBuffer": this.bufferSize = Integer.valueOf(c.getConfigValue()); break;
                case "searchInverse": this.searchInverse = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "pipeline": this.pipeline = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "targets": this.targets = c.getConfigValue(); if (targets.equals("|")) targets = ""; break;
                case "features": this.features = c.getConfigValue(); if (features.equals("|")) features = ".*"; break;
                default: log.warn("unknown parameter: " + c.getConfigKey()); break;
            }
        }
    }

    @Override
    protected void process(String s, Tuple tuple) throws CepTupleException, CepOperatorException
    {
        processData(tuple);
    }

    @Override
    public void emit(Tuple tuple) throws CepOperatorException
    {
        try {
            this.pairStream.send(tuple);
        }
        catch (CepStreamException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean isStateful()
    {
        return true;
    }

    @Override
    public String[] getRouteFields()
    {
        return new String[] {"GRIDCELL"};
    }

    @Override
    public void handleTimestampAdvance(long oldTS, long newTS, int eventsProcessed, int eventsDiscarded, long timeMs) throws CepOperatorException
    {
        long t0 = System.currentTimeMillis();

        log.info("INRIA: #" + t0 + "; Window slide(" + oldTS + "); Events: " + eventsProcessed + " processed, " + eventsDiscarded + " discarded in " + timeMs + "ms");

        if ( bufferSize == 0 ) {
            if (targets.length() == 0)
                flushCells();
            else {
                if (!pipeline)
                    flushCellsWithTargets(this.lastSystemTimestamp, oldTS);
            }
        }

        flushCounters(this.lastSystemTimestamp, oldTS);
        this.cellsWithTargets.clear();
        this.cellMap.clear();
    }
}
