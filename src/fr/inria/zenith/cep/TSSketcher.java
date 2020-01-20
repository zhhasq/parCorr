
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

public class TSSketcher extends AbstractCustomOperator implements IWindowCallback {

    private AbstractStream inStream;
    private AbstractStream sketchStream;
    private AbstractStream tsStreamCorr;
    private AbstractStream tsStreamVerif;
    private AbstractStream vocabularyStream;
    private FreeTuplePool freeTuplePool = FreeTuplePool.getFreeTuplePool();
    private static Logger log = Logger.getLogger(TSSketcher.class);

    private int windowSize;     // in seconds / number of subsequent events
    private int windowSlide = 1;
    private int timeUnit = 1;
    private int maxWindowLag = 0;
    private int sketchSize;
    private int gridDimensions;
    private int gridSize = 0;
    private int sampleSize = 0;
    private int vocabularyBuffer = 399;
    private double cellSize;
    private boolean searchInverse = false;
    private boolean candOnly = false;
    private boolean linearSearch = false;
    private String targets;
    private String duplAgg = null;

    private String strRandomMatrix;
    private int[][] randomMatrix = null;
    private int rmCols;         // extended windows

    private String strBreakpoints;

    private int currIndex = 0;
    private int slideStep = 0;

    private Object lastSystemTimestamp;
    private String lastEventId;

    private HashMap<String, TimeSeries> timeSeries = new HashMap<>();
    private HashMap<String, double[]> sketches = null;
    private double[][] breakpoints = null;
    private int[] rmsums = null;   // the sum of all random vectors in the current window, needed for incremental normal sketch computation

    private WindowControl windowControl = null;
    private HashSet<String> currentWindowEvents = new HashSet<>();

    private LinkedList<String> currentVocabulary = new LinkedList<>();

    public TSSketcher(Long subQueryInstanceID, String operatorName, java.util.ArrayList<AbstractStream> inStreams, ArrayList<AbstractStream> outStreams, AbstractStream timeoutStream)
    {
        super(subQueryInstanceID, operatorName, inStreams, outStreams, timeoutStream);

        log.info("INRIA: #" + this.hashCode() + " created");

        for (AbstractStream s : inStreams) {
            if (s.getStreamName().equals("inStream"))
                this.inStream = s;
            s.registerOperator(this);
        }

        for (AbstractStream s : outStreams) {
            if (s.getStreamName().equals("sketchStream"))
                this.sketchStream = s;
            else if (s.getStreamName().equals("tsStreamCorr"))
                this.tsStreamCorr = s;
            else if (s.getStreamName().equals("tsStreamVerif"))
                this.tsStreamVerif = s;
            else if (s.getStreamName().equals("vocabularyStream"))
                this.vocabularyStream = s;
        }

        this.vocabularyStream.setBroadcast(true);
    }

    private void parseRandomMatrix(String m)
    {
        if (m.length() < windowSize*sketchSize) {
            log.warn("inconsistent matrix size: " + m.length());
            return;
        }

        this.rmCols = m.length() / this.sketchSize;
        this.randomMatrix  = new int[sketchSize][rmCols];

        for ( int i = 0 ; i < this.sketchSize * this.rmCols ; ++i )
            this.randomMatrix[i / rmCols][i % rmCols] = (m.charAt(i) == '1' ? 1 : -1);
    }

    private void parseBreakpoints(String b)
    {
        String[] lines = b.split("\n");
        for ( int i = 0 ; i < sketchSize ; ++i ) {
            String[] values = lines[i].split(",");
            for (int k = 0 ; k < gridSize-1 ; ++k)
                breakpoints[i][k] = Double.valueOf(values[k]);
        }
    }

    private void printSketches()
    {
        for (HashMap.Entry<String, double[]> entry : this.sketches.entrySet()) {
            StringBuilder sb = new StringBuilder();
            sb.append(entry.getKey() + ": ");
            for (double v : entry.getValue())
                sb.append(v).append(" ");
            log.info("BOBBY: sketch of " + sb.toString());
        }
    }

    private double[] computeSketch(TimeSeries ts, int lag)
    {
        double[] sketch = new double[this.sketchSize];

        int startIndex = (this.currIndex - this.windowSize + 1 + this.rmCols) % this.rmCols;

        for ( int i = 0 ; i < this.sketchSize ; ++i ) {
            int j = startIndex;
            double sum = .0;
            for ( double v : ts.norm(lag) ) {
                sum += v * this.randomMatrix[i][j];
                if (++j == this.rmCols) j = 0;
            }
            sketch[i] = sum;
        }

        return sketch;
    }

    private void computeSketches()
    {
        rmsums = new int[this.sketchSize];

        for ( int i = 0 ; i < this.sketchSize ; ++i ) {
            rmsums[i] = 0;
            for ( int j = 0 ; j < this.windowSize ; ++j )
                rmsums[i] += randomMatrix[i][j];
        }

        for ( HashMap.Entry<String, TimeSeries> entry : timeSeries.entrySet() ) {
            String eventId = entry.getKey();
            TimeSeries ts = entry.getValue();

            this.sketches.put( eventId, computeSketch(ts, 0) );
        }
    }

    private void updateSketch(double[] sketch, int prevIndex, TimeSeries ts, int lag)
    {
        double newValue = ts.head(lag);
        double oldValue = ts.tail(lag);

        double[] currStats = ts.getStats(lag);
        double[] prevStats = ts.getStats(lag + 1);

        double newMean = currStats[0];
        double oldMean = prevStats[0];
        double newStdev = currStats[1];
        double oldStdev = prevStats[1];

        if (ts.isConst()) {
            for (int i = 0; i < this.sketchSize; ++i)
                sketch[i] = 0;

        } else {
            for (int i = 0; i < this.sketchSize; ++i) {
                double delta = (newValue - newMean) * this.randomMatrix[i][currIndex]
                        - (oldValue - oldMean) * this.randomMatrix[i][prevIndex]
                        - (rmsums[i] - this.randomMatrix[i][currIndex]) * (newMean - oldMean);

                sketch[i] = (sketch[i] * oldStdev + delta) / newStdev;
            }
        }

    }

    private void updateSketches()
    {
        int prevIndex = (this.currIndex - this.windowSize + this.rmCols) % this.rmCols;

        for ( int i = 0 ; i < this.sketchSize ; ++i )
            rmsums[i] += this.randomMatrix[i][currIndex] - this.randomMatrix[i][prevIndex];

        for ( HashMap.Entry<String, TimeSeries> entry : this.timeSeries.entrySet() ) {
            String eventId = entry.getKey();
            TimeSeries ts = entry.getValue();
            double[] sketch = this.sketches.get(eventId);

            if (sketch == null) {
                this.sketches.put( eventId, computeSketch(ts, 0) );

            } else {
                updateSketch(sketch, prevIndex, ts, 0);

                int maxLag = (int) Math.min((long)maxWindowLag, ts.getElemCount() - (long)windowSize );
                for ( int lag = 1 ; lag <= maxLag ; ++lag ) {
                    String eventId_L = eventId + "@-" + Integer.toString(lag);
                    double[] sketch_L = this.sketches.get( eventId_L );

                    if (sketch_L == null)
                        this.sketches.put( eventId_L, computeSketch(ts, lag) );
                    else
                        updateSketch(sketch_L, prevIndex, ts, lag);
                }
            }
        }

    }

    private void initBreakpoints()
    {

        this.breakpoints = new double[sketchSize][gridSize-1];

        if (strBreakpoints.length() > 0) {
            parseBreakpoints(strBreakpoints);
        } else {

            int sampleSize = (this.sampleSize > 0 ? Math.min(this.sketches.size(), this.sampleSize) : this.sketches.size()) / gridSize * gridSize;

            ArrayList<String> sh = new ArrayList<>( this.sketches.keySet() );
            Collections.shuffle(sh);

            double[][] m = new double[sketchSize][sampleSize];
            int i = 0;
            for (String tsId : sh)
            {
                double[] sketch = this.sketches.get(tsId);
                for (int j = 0; j < sketch.length; ++j)
                    m[j][i] = sketch[j];
                if (++i == sampleSize)
                    break;
            }

            for (int j = 0; j < sketchSize; ++j) {
                Arrays.sort(m[j]);
                for (int k = 1; k < gridSize; ++k)
                    breakpoints[j][k - 1] = m[j][sampleSize / gridSize * k];
            }
        }
    }

    private int breakpointCell(double[] bp, double v)
    {
        for (int k = 0 ; k < bp.length ; ++k)
            if (v <= bp[k])
                return k;
        return bp.length;
    }

    private void alignTimeSeries(long timestamp)
    {
        for ( HashMap.Entry<String, TimeSeries> entry : this.timeSeries.entrySet() ) {
            String eventId = entry.getKey();
            TimeSeries ts = entry.getValue();

            if ( !this.currentWindowEvents.contains(eventId) )
                ts.add(ts.head(), timestamp);
        }

        this.currentWindowEvents.clear();
    }

    private void emitEventCell( String eventId, int dims[], Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("EVENTID", eventId);
        t.addField("GRIDCELL", Utils.intsToString(dims));
        emit(t);
    }

    private void emitSketch(String eventId, double[] sketch, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        for ( int groupId = 0 ; groupId < this.sketchSize / this.gridDimensions ; ++groupId ) {

            int dims[] = new int[this.gridDimensions + 1];
            int dims_inv[] = new int[this.gridDimensions + 1];

            for ( int dim = 0 ; dim < this.gridDimensions ; ++dim ) {
                double v = sketch[groupId * this.gridDimensions + dim];
                if (gridSize > 0) {
                    dims[dim] = breakpointCell(breakpoints[groupId * gridDimensions + dim], v);
                    dims_inv[dim] = gridSize - 1 - dims[dim];
                } else {
                    double d = v  / this.cellSize;
                    dims[dim] = (int) d - (d < 0 ? 1 : 0);
                    dims_inv[dim] = -dims[dim] - 1;
                }
            }

            dims[this.gridDimensions] = groupId;
            dims_inv[this.gridDimensions] = groupId;

            emitEventCell( eventId, dims, systemTimestamp, userTimestamp );

            if ( searchInverse /* && !this.timeSeries.get(eventId).isConst() */ )
                emitEventCell( eventId + "-", dims_inv, systemTimestamp, userTimestamp );
        }

    }

    private void emitSketches( Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        if (targets.length() > 0) {
            for (HashMap.Entry<String, double[]> entry : this.sketches.entrySet()) {
                String eventId = entry.getKey();

                if (eventId.matches(targets))
                    emitSketch(eventId, entry.getValue(), systemTimestamp, userTimestamp);
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            for (HashMap.Entry<String, double[]> entry : this.sketches.entrySet()) {
                String eventId = entry.getKey();

                if (!eventId.matches(targets))
                    emitSketch(eventId, entry.getValue(), systemTimestamp, userTimestamp);
            }
        } else {
            for (HashMap.Entry<String, double[]> entry : this.sketches.entrySet())
                emitSketch(entry.getKey(), entry.getValue(), systemTimestamp, userTimestamp);
        }
    }

    private void addVocabulary(String eventId, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        currentVocabulary.add(eventId);
        if (currentVocabulary.size() >= vocabularyBuffer)
            flushVocabulary(systemTimestamp, userTimestamp);
    }

    private void flushVocabulary(Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        if (currentVocabulary.size() == 0)
            return;

        StringJoiner joiner = new StringJoiner("|");
        for (String e : currentVocabulary)
            joiner.add(e);

        broadcastEventId(joiner.toString(), systemTimestamp, userTimestamp);
        currentVocabulary.clear();
    }

    private void broadcastEventId( String eventId, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("EVENT", eventId);

        try {
            this.vocabularyStream.send(t);
        }
        catch (CepStreamException e) {
            e.printStackTrace();
        }
    }

    private void emitTimeSeries( Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        for ( HashMap.Entry<String, TimeSeries> entry : this.timeSeries.entrySet() ) {
            String eventId = entry.getKey();
            TimeSeries ts = entry.getValue();

            double[] win;
            if ( ts.getElemCount() < windowSize + windowSlide )
                win = ts.headArray(windowSize);
            else
                win = ts.headArray(windowSlide);

            Tuple t = this.freeTuplePool.pop();
            t.addField("cep_system_timestamp", systemTimestamp);
            t.addField("cep_user_timestamp", userTimestamp);
            t.addField("EVENT", eventId);
            t.addField("VECTOR", Utils.doublesToBytes(win));

            try {
                this.tsStreamCorr.send(t);
                this.tsStreamVerif.send(t);
            }
            catch (CepStreamException e) {
                e.printStackTrace();
            }
        }
    }

    private void processData(Tuple tuple) throws CepTupleException, CepOperatorException
    {
        this.lastSystemTimestamp = tuple.getField("cep_system_timestamp");
        long userTS = (Long) tuple.getField("cep_user_timestamp");
        String eventId = (String) tuple.getField("EVENT");
        Double value = (Double) tuple.getField("VALUE");

        this.lastEventId = eventId;
        if ( !windowControl.handleEvent( userTS, false ) )
            return;

        this.currentWindowEvents.add(eventId);

        TimeSeries ts = this.timeSeries.get(eventId);
        if ( ts != null )
            ts.add( value, userTS );

        else {
            ts = new TimeSeries( this.windowSize, maxWindowLag, value, userTS, duplAgg );

            int pad = this.sketches == null ? this.currIndex : this.windowSize /* intentionally (not windowSize-1), in order to trigger ts.computeStats() */;
            for ( int i = 0 ; i < pad ; ++i )
                ts.add( value, userTS - (pad - i) * timeUnit );

            this.timeSeries.put( eventId, ts );

            if ( linearSearch )
                addVocabulary(eventId, lastSystemTimestamp, userTS);
        }
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
                case "windowSize": this.windowSize = Integer.valueOf(c.getConfigValue()); break;
                case "windowSlide": this.windowSlide = Integer.valueOf(c.getConfigValue()); break;
                case "timeUnit": this.timeUnit = Integer.valueOf(c.getConfigValue()); break;
                case "maxWindowLag": this.maxWindowLag = Integer.valueOf(c.getConfigValue()); break;
                case "sketchSize": this.sketchSize = Integer.valueOf(c.getConfigValue()); break;
                case "gridDimensions": this.gridDimensions = Integer.valueOf(c.getConfigValue()); break;
                case "gridSize": this.gridSize = Integer.valueOf(c.getConfigValue()); break;
                case "sampleSize": this.sampleSize = Integer.valueOf(c.getConfigValue()); break;
                case "vocabularyBuffer": this.vocabularyBuffer = Integer.valueOf(c.getConfigValue()); break;
                case "cellSize": this.cellSize = Double.valueOf(c.getConfigValue()); break;
                case "searchInverse": this.searchInverse = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "candOnly": this.candOnly = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "linearSearch": this.linearSearch = (c.getConfigValue().equalsIgnoreCase("true")); break;
                case "targets": this.targets = c.getConfigValue(); if (targets.equals("|")) targets = ""; break;
                case "duplAgg": this.duplAgg = c.getConfigValue(); break;
                case "randomMatrix": this.strRandomMatrix = c.getConfigValue(); break;
                case "bpstr": this.strBreakpoints = c.getConfigValue(); if (strBreakpoints.equals("|")) strBreakpoints = ""; break;
                default: log.warn("unknown parameter: " + c.getConfigKey()); break;
            }
        }

        parseRandomMatrix(this.strRandomMatrix);

        this.windowControl =  new WindowControl(this, timeUnit);

        log.info("INRIA: cellSize=" + cellSize + ", gridSize=" + gridSize + ", gridDimensions=" + gridDimensions + ", candOnly=" + candOnly);
    }

    @Override
    protected void process(String s, Tuple tuple) throws CepTupleException, CepOperatorException {
        processData(tuple);
    }

    @Override
    public void emit(Tuple tuple) throws CepOperatorException {
        try {
            this.sketchStream.send(tuple);
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
    public void handleTimestampAdvance(long oldTS, long newTS, int eventsProcessed, int eventsDiscarded, long timeMs) throws CepOperatorException {

        if (linearSearch) {
            flushVocabulary(lastSystemTimestamp, oldTS);
        }

        alignTimeSeries(oldTS);

        long ts = System.currentTimeMillis();

        if ( this.sketches == null ) {
            if ( this.currIndex == this.windowSize - 1 ) {
                sketches = new HashMap<>();
                if ( !linearSearch )
                    computeSketches();
            }

        } else if ( !linearSearch )
            updateSketches();

        if (++this.currIndex == this.rmCols) this.currIndex = 0;

        if ( this.sketches != null ) {

            if (this.slideStep == 0) {
                if (!linearSearch && gridSize > 0)
                    initBreakpoints();

                log.info("INRIA: #" + ts + "; Window slide(" + oldTS + "); Events: " + eventsProcessed + " processed, " + eventsDiscarded + " discarded in " + timeMs + "ms");

                windowControl.resetCounters();

                if ( !candOnly )
                    emitTimeSeries( lastSystemTimestamp, oldTS );

                if ( !linearSearch )
                    emitSketches( lastSystemTimestamp, oldTS );
            }

            if (++this.slideStep == this.windowSlide) this.slideStep = 0;
        }

    }
}
