
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

public class TimeSeries {

    private int windowSize;

    private double[] data;
    private int dataSize;
    private int dataIndex = -1;

    private double[] stats;
    private int statsSize;
    private int statsIndex = -1;

    private double mean;
    private double var;

    private long elemCount = 0;
    private long lastTS = -1;

    private int duplCnt = 0;
    private String duplAgg = null;

    private void init( int windowSize, int maxLag )
    {
        this.windowSize = windowSize;

        this.dataSize = windowSize + maxLag + 1;
        this.data = new double[dataSize];

        this.statsSize = maxLag + 2;
        this.stats = new double[statsSize * 2];
    }

    private void init( int windowSize, int maxLag, double initValue, long initTS )
    {
        init(windowSize, maxLag);
        add(initValue, initTS);
    }

    public TimeSeries( int windowSize, int maxLag )
    {
        init(windowSize, maxLag);
    }

    public TimeSeries( int windowSize, int maxLag, double initValue, long initTS )
    {
        init(windowSize, maxLag, initValue, initTS);
    }

    public TimeSeries( int windowSize, int maxLag, double initValue, long initTS, String duplAgg )
    {
        init(windowSize, maxLag, initValue, initTS);

        this.duplAgg = duplAgg;
    }

    // TODO: implement time unit logic

    private int currWinSize(int lag)
    {
        return Math.max((int) Math.min((long) windowSize, elemCount-lag), 0);
    }

    private void computeStats()
    {
        double sum = 0;
        double sumsq = 0;

        for ( int i = 0 ; i < windowSize ; ++i ) {
            sum += data[i];
            sumsq += data[i] * data[i];
        }

        mean = sum / windowSize;
        var = sumsq / windowSize - mean * mean;

        this.statsIndex = 0;
        stats[0] = mean;
        stats[1] = var < 0 ? 0.0 : Math.sqrt( var );
    }

    private void updateStats()
    {
        double prevMean = mean;

        mean += ( head() - tail() ) / windowSize;
        var += ( mean - prevMean ) * ( head() + tail() - ( mean + prevMean ) );

        if (++this.statsIndex == this.statsSize) this.statsIndex = 0;

        stats[2 * statsIndex] = mean;
        stats[2 * statsIndex + 1] = var < 0 ? 0.0 : Math.sqrt( var );
    }

    private void accumulateStats()
    {
        if ( elemCount == 1 ) {
            mean = head();
            var = 0;

        } else {
            double prevMean = mean;
            double prevVar = var;

            if ( elemCount <= windowSize ) {
                // accumulate
                mean = ((elemCount-1)*prevMean + head()) / elemCount;
                var = ((elemCount-1)*prevVar + Math.pow(head(), 2) - elemCount*Math.pow(mean, 2) + (elemCount-1)*Math.pow(prevMean, 2)) / elemCount;
            } else {
                // update
                mean += ( head() - tail() ) / windowSize;
                var += ( mean - prevMean ) * ( head() + tail() - ( mean + prevMean ) );
            }

            if (++this.statsIndex == this.statsSize) this.statsIndex = 0;

            stats[2 * statsIndex] = mean;
            stats[2 * statsIndex + 1] = var < 0 ? 0.0 : Math.sqrt( var );
        }
    }

    public long getElemCount()
    {
        return elemCount;
    }

    public long getLastTS()
    {
        return lastTS;
    }

    public double[] subArray(int di, int elems)
    {
        double[] a = new double[elems];

        int i = 0;
        while ( i < elems ) {
            a[i++] = data[di];
            if (++di == this.dataSize) di = 0;
        }

        return a;
    }

    public double[] headArray(int elems)
    {
        int di = (dataIndex - elems + 1 + dataSize) % dataSize;
        return subArray(di, elems);
    }

    // to be used only when elemCount > windowSize
    public double[] tailArray(int elems)
    {
        int di = (dataIndex - windowSize - elems + 1 + dataSize) % dataSize;
        return subArray(di, elems);
    }

    public double head(int lag)
    {
        return data[(dataIndex - lag + dataSize) % dataSize];
    }

    // to be used only when elemCount > windowSize
    public double tail(int lag)
    {
        return data[(dataIndex - windowSize - lag + dataSize) % dataSize];
    }

    public double head()
    {
        return head(0);
    }

    public double tail()
    {
        return tail(0);
    }

    public void add( double value, long timestamp )
    {
        if (this.duplAgg != null && this.lastTS == timestamp) {
            ++duplCnt;

            if (this.duplAgg.equalsIgnoreCase("LAST"))
                data[dataIndex] = value;
            else if (this.duplAgg.equalsIgnoreCase("SUM"))
                data[dataIndex] += value;
            else if (this.duplAgg.equalsIgnoreCase("AVG"))
                data[dataIndex] = (data[dataIndex]*duplCnt + value) / (duplCnt+1);
            else if (this.duplAgg.equalsIgnoreCase("MIN"))
            { if (data[dataIndex] > value) data[dataIndex] = value; }
            else if (this.duplAgg.equalsIgnoreCase("MAX"))
            { if (data[dataIndex] < value) data[dataIndex] = value; }

            return;
        }

        if (++this.dataIndex == this.dataSize) this.dataIndex = 0;
        ++elemCount;
        this.lastTS = timestamp;
        duplCnt = 0;

        data[dataIndex] = value;

        accumulateStats();
        /*
        if ( statsIndex == -1 && dataIndex == windowSize - 1 )
            computeStats();

        else if ( statsIndex >= 0 )
            updateStats();
        */
    }

    public double[] arrayWithStats(int lag)
    {
        int ws = currWinSize(lag);

        double[] a = new double[ws + 2];

        int di = (dataIndex - ws + 1 - lag + dataSize) % dataSize;
        int si = (statsIndex - lag + statsSize) % statsSize;

        int i = 0;
        while ( i < ws ) {
            a[i++] = data[di];
            if (++di == this.dataSize) di = 0;
        }

        a[i++] = stats[2 * si];
        a[i] = stats[2 * si + 1];

        return a;
    }

    public double[] arrayWithStats()
    {
        return arrayWithStats(0);
    }

    public double[] norm(int lag)
    {
        int ws = currWinSize(lag);

        double[] a = new double[ws];

        int di = (dataIndex - ws + 1 - lag + dataSize) % dataSize;
        int si = (statsIndex - lag + statsSize) % statsSize;

        double mean = stats[2 * si];
        double stdev = stats[2 * si + 1];

        int i = 0;
        while ( i < ws ) {
            a[i++] = isConst(lag) ? 0 : (data[di] - mean) / stdev;
            if (++di == this.dataSize) di = 0;
        }

        return a;
    }

    public double[] norm()
    {
        return norm(0);
    }

    public double[] getStats(int lag)
    {
        int si = (statsIndex - lag + statsSize) % statsSize;

        return new double[] { stats[2 * si], stats[2 * si + 1] };
    }

    public double getMean(int lag)
    {
        return getStats(lag)[0];
    }

    public double getMean()
    {
        return getMean(0);
    }

    public double getStdev(int lag)
    {
        return getStats(lag)[1];
    }

    public double getStdev()
    {
        return getStdev(0);
    }

    public boolean isConst(int lag)
    {
        double epsilon = 0.00001;
        int si = (statsIndex - lag + statsSize) % statsSize;
        return stats[2 * si + 1] < epsilon;
    }

    public boolean isConst()
    {
        return isConst(0);
    }
}
