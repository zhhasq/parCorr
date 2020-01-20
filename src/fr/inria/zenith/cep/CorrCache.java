
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import java.util.HashMap;

public class CorrCache {
    private int windowSize;
    private int maxLag;

    private HashMap<String, Pair<Long, TimeSeries>> timeSeries = new HashMap<>(); // id -> (initialIndex, ts)
    private HashMap<String, Pair<Long, Double>> lastCorr = new HashMap<>();       // "target_id|feature_id" -> (lastTargetIndex, lastCorr)

    public CorrCache(int windowSize, int maxLag)
    {
        this.windowSize = windowSize;
        this.maxLag = maxLag;
    }

    public Double process(long index, String targetId, double[] targetData, String featureId, double[] featureData)
    {
        TimeSeries targetTS = pokeCache(index, targetId, targetData);
        TimeSeries featureTS = pokeCache(index, featureId, featureData);

        Double corr;
        String corrId = targetId + "|" + featureId;

        Pair<Long, Double> entry = lastCorr.get(corrId);
        long offset = entry == null ? 0 : index - entry.getFirst();

        if ( targetTS.getElemCount() <= windowSize || featureTS.getElemCount() <= windowSize ) {

            if ( targetTS.getElemCount() != featureTS.getElemCount() )
                return null;

            if (entry == null || offset > windowSize || offset > maxLag) {
                corr = Utils.corrWithStats( targetTS.arrayWithStats(), featureTS.arrayWithStats() );
            } else {
                // accumulate corr using cache
                double prevCorr = entry.getSecond();
                corr = accumulateCorr( targetTS, featureTS, prevCorr, (int)offset );
            }

        } else if (entry == null || targetTS.getElemCount() < windowSize + offset || featureTS.getElemCount() < windowSize + offset || offset > windowSize || offset > maxLag) {
            corr = Utils.corrWithStats( targetTS.arrayWithStats(), featureTS.arrayWithStats() );

        } else {
            // update corr using cache
            double prevCorr = entry.getSecond();
            corr = accumulateCorr( targetTS, featureTS, prevCorr, (int)offset );
        }

        if (corr != null)
            lastCorr.put(corrId, new Pair<>(index, corr));

        return corr;
    }

    private TimeSeries pokeCache(long index, String id, double[] data)
    {
        long initialIndex = index - (data.length - 2);
        if ( initialIndex < 0 )
            // should never happen; TODO: log some warning
            initialIndex = 0;

        TimeSeries ts;
        int elemsToAdd;

        Pair<Long, TimeSeries> entry = timeSeries.get(id);
        long entryIndex = entry == null ? 0 : entry.getSecond().getElemCount() + entry.getFirst();

        if ( entry == null || entryIndex < initialIndex ) {
            ts = new TimeSeries(windowSize, maxLag);
            timeSeries.put(id, new Pair<>(initialIndex, ts));
            elemsToAdd = data.length - 2;
        } else {
            ts = entry.getSecond();
            elemsToAdd = (int)(index - entryIndex);
        }

        for ( int i = data.length - 2 - elemsToAdd ; i < data.length - 2 ; ++i )
            ts.add(data[i], initialIndex + i);

        return ts;
    }

    private Double accumulateCorr(TimeSeries targetTS, TimeSeries featureTS, double prevCorr, int offset)
    {
        boolean update = targetTS.getElemCount() > windowSize;

        int ws = update ? windowSize : (int)targetTS.getElemCount();
        int overlap = ws - offset;

        double t_mean_new = targetTS.getMean();
        double t_mean_old = targetTS.getMean(offset);
        double t_stdev_new = targetTS.getStdev();
        double t_stdev_old = targetTS.getStdev(offset);

        double f_mean_new = featureTS.getMean();
        double f_mean_old = featureTS.getMean(offset);
        double f_stdev_new = featureTS.getStdev();
        double f_stdev_old = featureTS.getStdev(offset);

        if (f_stdev_new < Utils.epsilon || t_stdev_new < Utils.epsilon)
            return null;

        double[] t_new = targetTS.headArray(offset);
        double[] f_new = featureTS.headArray(offset);

        double t_overlap = t_mean_new * ws - Utils.sum(t_new);
        double f_overlap = f_mean_new * ws - Utils.sum(f_new);

        double delta = overlap * (t_mean_new * f_mean_new - t_mean_old * f_mean_old)
                - (t_mean_new - t_mean_old) * f_overlap
                - (f_mean_new - f_mean_old) * t_overlap;

        delta += Utils.product( Utils.add(t_new, -t_mean_new), Utils.add(f_new, -f_mean_new) );

        if (update) {
            double[] t_old = targetTS.tailArray(offset);
            double[] f_old = featureTS.tailArray(offset);

            delta -= Utils.product( Utils.add(t_old, -t_mean_old), Utils.add(f_old, -f_mean_old) );
        }

        return ( delta  +  prevCorr * (update ? ws : overlap) * t_stdev_old * f_stdev_old )
                / ( ws * t_stdev_new * f_stdev_new );
    }

}
