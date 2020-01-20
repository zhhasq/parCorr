
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import es.upm.cep.commons.exception.CepOperatorException;
import es.upm.cep.commons.exception.CepStreamException;
import es.upm.cep.commons.leancollection.FreeTuplePool;
import es.upm.cep.core.stream.AbstractStream;
import es.upm.cep.commons.type.Tuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.StringJoiner;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class Regression {

    private AbstractStream targetStream;
    private int windowSize;
    private int nsamples = 0;
    private int topk = 10;
    private boolean skipTraining = false;

    private FreeTuplePool freeTuplePool = FreeTuplePool.getFreeTuplePool();

    // target -> [(corr, (event, ts)), ...]
    // the first in the row is the target itself
    HashMap< String, LinkedList< Pair< Double, Pair<String, byte[]> > > > targetMap = new HashMap<>();

    public Regression( AbstractStream targetStream, int windowSize, int maxSamples, int maxFeatures, boolean skipTraining )
    {
        this.targetStream = targetStream;
        this.windowSize = windowSize;
        this.nsamples = (maxSamples <= 0 || maxSamples > windowSize) ? windowSize : maxSamples;
        this.topk = maxFeatures;
        this.skipTraining = skipTraining;
    }

    private Pair< Double, Pair<String, byte[]> > createTargetEntry(String event, double corr, byte[] data) {
        return new Pair<>(corr, new Pair<>(event, data));
    }

    public void addTargetEvent(String target, String event, double corr, byte[] target_ts, byte[] event_ts) {
        LinkedList< Pair< Double, Pair<String, byte[]> > > events = this.targetMap.get(target);

        if ( events == null ) {
            events = new LinkedList<>();
            events.add(createTargetEntry(target, 1, target_ts));
        }

        ListIterator< Pair< Double, Pair<String, byte[]> > > it = events.listIterator();
        boolean isInserted = false;

        while (!isInserted && it.hasNext()) {
            if (Math.abs(corr) > Math.abs(it.next().getFirst()) ) {
                it.previous();
                it.add(createTargetEntry(event, corr, event_ts));
                it.next();
                isInserted = true;
            }
        }

        if (isInserted) {
            if (topk > 0 && events.size() > topk + 1)
                events.removeLast();
        } else {
            if (topk == 0 || events.size() <= topk)
                events.add(createTargetEntry(event, corr, event_ts));
        }

        this.targetMap.put(target, events);
    }

    private void flushTargetsData(long sysTS, long userTS) throws CepOperatorException {
        // this outputs targets and features so that the receiver app can visualize (prepared for the demo at review 2018)
        for ( HashMap.Entry<String, LinkedList< Pair< Double, Pair<String, byte[]> > > > entry : this.targetMap.entrySet() ) {
            String target = entry.getKey();
            int numEvents = entry.getValue().size();
            int tsSize = windowSize * 8; // size of double
            byte[] data = new byte[tsSize * numEvents];

            int i = 0;
            StringJoiner joiner = new StringJoiner("|");
            for (Pair< Double, Pair<String, byte[]> > e : entry.getValue()) {
                Pair<String, byte[]> eventWithData = e.getSecond();
                System.arraycopy(eventWithData.getSecond(), 0, data, (i++) * tsSize, tsSize);
                joiner.add(eventWithData.getFirst());
            }
            String events = joiner.toString();

            emitTarget(target, events, data, sysTS, userTS);
        }
    }

    private double[] regress( double[] targetData, double[][] predictorsData ) {
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(targetData, predictorsData);
        return regression.estimateRegressionParameters();
    }

    private void processTargets(long sysTS, long userTS) throws CepOperatorException {
        for ( HashMap.Entry<String, LinkedList< Pair< Double, Pair<String, byte[]> > > > entry : this.targetMap.entrySet() ) {
            String target = entry.getKey();
            int numEvents = entry.getValue().size()-1;

            double[] targetData = new double[nsamples];
            double[][] predictorsData = new double[nsamples][numEvents];
            double[] corrs = new double[numEvents];

            int i = 0;
            StringJoiner joiner = new StringJoiner("|");
            for (Pair< Double, Pair<String, byte[]> > e : entry.getValue()) {
                Pair<String, byte[]> eventWithData = e.getSecond();
                double[] eventData = Utils.bytesToDoubles( eventWithData.getSecond() );

                if ( i == 0 )
                    for ( int j = windowSize - nsamples ; j < windowSize ; ++j )
                        targetData[j - (windowSize - nsamples)] = eventData[j];
                else {
                    for ( int j = windowSize - nsamples ; j < windowSize ; ++j )
                        predictorsData[j - (windowSize - nsamples)][i-1] = eventData[j];
                    joiner.add( eventWithData.getFirst() );
                    corrs[i-1] = e.getFirst();
                }
                ++i;
            }
            String predictors = joiner.toString();

            if (skipTraining) {
                emitRegression(target, predictors, corrs, sysTS, userTS);
            } else {
                double[] coefs = regress(targetData, predictorsData);
                emitRegression(target, predictors, coefs, sysTS, userTS);
            }
        }
    }

    public void flushTargets(long sysTS, long userTS) throws CepOperatorException {
        if (targetStream.getSchema().getFields().containsKey("DATA"))
            flushTargetsData(sysTS, userTS);
        else
            processTargets(sysTS, userTS);

        targetMap.clear();
    }

    private void emitTarget( String target, String events, byte[] data, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("TARGET", target);
        t.addField("EVENTS", events);
        t.addField("DATA", data);

        try {
            this.targetStream.send(t);
        }
        catch (CepStreamException e) {
            e.printStackTrace();
        }
    }

    private void emitRegression( String target, String predictors, double[] coefs, Object systemTimestamp, Object userTimestamp ) throws CepOperatorException
    {
        StringJoiner joiner = new StringJoiner("|");
        for (double c : coefs)
            joiner.add(String.valueOf(c));
        String strCoefs = joiner.toString();

        Tuple t = this.freeTuplePool.pop();
        t.addField("cep_system_timestamp", systemTimestamp);
        t.addField("cep_user_timestamp", userTimestamp);
        t.addField("timestamp", userTimestamp);
        t.addField("TARGET", target);
        t.addField("PREDICTORS", predictors);
        t.addField("COEFS", strCoefs);

        try {
            this.targetStream.send(t);
        }
        catch (CepStreamException e) {
            e.printStackTrace();
        }
    }

}
