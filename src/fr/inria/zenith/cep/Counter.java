
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import es.upm.cep.commons.exception.CepOperatorException;

import java.util.LinkedList;
import java.util.ListIterator;
import java.util.StringJoiner;

public class Counter {
    public interface SerializeCondition {
        boolean test(String s);
    }

    private LinkedList< Pair< String, Integer > > entries = new LinkedList<>();     // ordered list

    private ICounterCallback cb = null;
    private String extkey;
    private int threshold;

    public Counter() {}

    public Counter( String key )
    {
        entries.add( new Pair<>( key, 1 ) );
    }

    public Counter( String key, int count )
    {
        entries.add( new Pair<>( key, count ) );
    }

    public void setCallback( ICounterCallback cb, String extkey, int threshold )
    {
        this.cb = cb;
        this.extkey = extkey;
        this.threshold = threshold;
    }

    public void merge( ListIterator< Pair< String, Integer > > it_that ) throws CepOperatorException {
        ListIterator< Pair< String, Integer > > it_this = entries.listIterator();

        while ( it_this.hasNext() && it_that.hasNext() ) {
            Pair< String, Integer > this_pair = it_this.next();
            Pair< String, Integer > that_pair = it_that.next();

            int diff = that_pair.getFirst().compareTo(this_pair.getFirst());
            if ( diff < 0 ) {
                it_this.previous();
                it_this.add( new Pair<>(that_pair.getFirst(), that_pair.getSecond()) );

                if ( this.cb != null && that_pair.getSecond() >= this.threshold )
                    cb.handleCounterThreshold( extkey, that_pair.getFirst() );

            } else if ( diff == 0 ) {
                int oldValue = this_pair.getSecond();
                int newValue = oldValue + that_pair.getSecond();
                this_pair.setSecond( newValue );

                if ( this.cb != null && oldValue < this.threshold && newValue >= this.threshold )
                    cb.handleCounterThreshold( extkey, this_pair.getFirst() );

            } else {
                it_that.previous();
            }
        }

        while ( it_that.hasNext() ) {
            Pair< String, Integer > that_pair = it_that.next();
            entries.add( new Pair<>(that_pair.getFirst(), that_pair.getSecond()) );

            if ( this.cb != null && that_pair.getSecond() >= this.threshold )
                cb.handleCounterThreshold( extkey, that_pair.getFirst() );
        }
    }

    public void merge( Counter that ) throws CepOperatorException {
        merge( that.entries.listIterator() );
    }


    public void add( String key ) throws CepOperatorException {
        merge( new Counter(key) );
    }

    public void add( String key, int count ) throws CepOperatorException {
        merge( new Counter(key, count) );
    }

    public void addEntry( String key, int count ) throws CepOperatorException {
        this.entries.add( new Pair<>(key, count) );
        if ( this.cb != null && count >= this.threshold )
            cb.handleCounterThreshold( extkey, key );
    }

    public String serialize(SerializeCondition cond)
    {
        StringJoiner joiner = new StringJoiner("|");
        for ( Pair< String, Integer > entry : entries ) {
            if ( cond.test(entry.getFirst()) )
                joiner.add( entry.getFirst() + ":" + entry.getSecond() );
        }
        return joiner.toString();
    }

    public String serialize()
    {
        return serialize(s -> true);
    }

    private static void parseKeyCount( String str, Counter c ) throws CepOperatorException {
        int pos = str.indexOf(':');
        c.addEntry( str.substring(0, pos), Integer.valueOf(str.substring(pos+1)) ) ;
    }

    private static void deserialize( String str, Counter c ) throws CepOperatorException {
        int prev = 0;
        int curr = str.indexOf('|', prev);

        while (curr > 0) {
            parseKeyCount( str.substring(prev, curr), c );
            prev = curr + 1;
            curr = str.indexOf('|', prev);
        }
        parseKeyCount( str.substring(prev), c );

    }

    public static Counter deserialize( String str ) throws CepOperatorException {
        Counter ret = new Counter();
        deserialize(str, ret);
        return ret;
    }

    public static Counter deserialize( String str, ICounterCallback cb, String extkey, int threshold ) throws CepOperatorException {
        Counter ret = new Counter();
        ret.setCallback(cb, extkey, threshold);
        deserialize(str, ret);
        return ret;
    }

    public void clear()
    {
        entries.clear();
    }

    public int size() {
        return entries.size();
    }
}
