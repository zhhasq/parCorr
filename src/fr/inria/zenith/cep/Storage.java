
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import es.upm.cep.commons.exception.CepOperatorException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedList;

public class Storage {

    private class Query {

        long timestamp;

        String compositeEventId;
            String eventId;
            int lag;
            boolean inverse;

        Object callbackObject;

        Query( long timestamp, String compositeEventId, Object callbackObject )
        {
            this.timestamp = timestamp;
            this.compositeEventId = compositeEventId;
            this.callbackObject = callbackObject;

            if (compositeEventId.endsWith("-")) {
                this.inverse = true;
                this.eventId = compositeEventId.substring(0, compositeEventId.length()-1);
            } else {
                this.inverse = false;
                this.eventId = compositeEventId;
            }

            int lagsep = this.eventId.indexOf("@-");
            if ( lagsep == -1 )
                this.lag = 0;
            else {
                this.lag = Integer.valueOf(this.eventId.substring(lagsep + 2));
                this.eventId = this.eventId.substring(0, lagsep);
            }
        }
    }

    private class Entry {
        TimeSeries ts = new TimeSeries(windowSize, maxLag);
        LinkedList<Query> queryQueue = new LinkedList<>();
        LinkedList<Pair<Long, Long>> timestampHistory = new LinkedList<>();

        long getIndex( long timestamp )
        {
            while (!timestampHistory.isEmpty()) {
                Pair<Long, Long> index = timestampHistory.getFirst();
                if ( index.getFirst() == timestamp )
                    return index.getSecond();

                if ( index.getFirst() > timestamp )
                    break;

                timestampHistory.pollFirst();
            }

            return -1;
        }
    }

    private IJoinCallback cb;
    private int windowSize;
    private int maxLag;

    private static Logger log = Logger.getLogger(Storage.class);

    private HashMap<String, Entry> storage = new HashMap<>();

    public Storage( IJoinCallback cb )
    {
        this.cb = cb;
    }

    public void setConfig( int windowSize, int maxLag )
    {
        this.windowSize = windowSize;
        this.maxLag = maxLag;
    }

    private void processQuery( Query query, Entry entry ) throws CepOperatorException
    {
        long index = entry.getIndex( query.timestamp );
        if ( index == -1 ) // TODO: log some warning
            return;

        double[] data = entry.ts.arrayWithStats( (int) (entry.ts.getElemCount() - index) + query.lag );

        if (query.inverse) {
            for ( int i = 0 ; i < data.length - 1 ; ++i )
                data[i] = -data[i];
        }

        cb.handleJoinTuple( query.timestamp, index, query.compositeEventId, data, query.callbackObject );
    }

    public TimeSeries putTS( long timestamp, String eventId, double[] data ) throws CepOperatorException
    {
        Entry entry = storage.computeIfAbsent(eventId, k -> new Entry());

        for ( double v : data )
            entry.ts.add(v, timestamp);

        entry.timestampHistory.offerLast( new Pair<>(timestamp, entry.ts.getElemCount()) );

        for ( Query query : entry.queryQueue )
            processQuery( query, entry );

        entry.queryQueue.clear();

        return entry.ts;
    }

    public void queryTS( long timestamp, String compositeEventId, Object callbackObject ) throws CepOperatorException
    {
        Query query = new Query(timestamp, compositeEventId, callbackObject);
        Entry entry = storage.computeIfAbsent(query.eventId, k -> new Entry());

        if ( entry.timestampHistory.isEmpty() || entry.timestampHistory.getLast().getFirst() < timestamp )
            entry.queryQueue.offerLast(query);
        else
            processQuery(query, entry);
    }

}
