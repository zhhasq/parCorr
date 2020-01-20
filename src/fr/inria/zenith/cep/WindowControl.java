
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import es.upm.cep.commons.exception.CepOperatorException;

public class WindowControl {

    private IWindowCallback cb;
    private int timeUnit = 1;

    private long currTS = -1;
    private int eventsProcessed = 0;
    private int eventsDiscarded = 0;

    private long startMillis;
    private long currMillis;

    public WindowControl( IWindowCallback cb )
    {
        this.cb = cb;
    }

    public WindowControl( IWindowCallback cb, int timeUnit )
    {
        this.cb = cb;
        this.timeUnit = timeUnit;
    }

    public boolean handleEvent( long timestamp, boolean autoReset ) throws CepOperatorException {
        if ( currTS > timestamp ) {
            ++eventsDiscarded;
            return false;
        }

        if ( currTS == -1 ) {
            currTS = timestamp;
            startMillis = System.currentTimeMillis();
        }

        if ( timestamp - currTS >= timeUnit ) {
            cb.handleTimestampAdvance( currTS, timestamp, eventsProcessed, eventsDiscarded, currMillis - startMillis );

            currTS = timestamp;

            if ( autoReset )
                resetCounters();
        }

        ++eventsProcessed;
        currMillis = System.currentTimeMillis();

        return true;
    }

    public void resetCounters()
    {
        eventsProcessed = 0;
        eventsDiscarded = 0;
        startMillis = System.currentTimeMillis();
    }
}
