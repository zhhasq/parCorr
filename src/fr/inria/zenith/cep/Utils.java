
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    public static final double epsilon = 0.00000001;

    public static byte[] intsToBytes(int[] v)
    {
        byte[] b = new byte[v.length * 4];

        for (int i = 0 ; i < v.length ; ++i)
            for (int j = 0 ; j < 4 ; ++j)
                b[i*4+j] = (byte) (v[i] >> (3-j)*8);

        return b;
    }
    
    public static int[] bytesToInts(byte[] b)
    {
        int[] v = new int[b.length / 4];

        for (int i = 0 ; i < v.length ; ++i) {
            v[i] = 0;
            for (int j = 0 ; j < 4 ; ++j)
                v[i] |= (b[i*4+j] & 0xFF) << (3-j)*8;
        }

        return v;
    }

    public static String intsToString(int[] v)
    {
        StringBuilder s = new StringBuilder();

        for (int i = 0 ; i < v.length ; ++i) {
            if ( i > 0 )
                s.append("|");
            s.append(v[i]);
        }

        return s.toString();
    }

    public static String doublesToString(double[] v)
    {
        StringBuilder s = new StringBuilder();

        s.append("[ ");
        for (double d : v) {
            s.append(d);
            s.append(" ");
        }
        s.append("]");

        return s.toString();
    }

    public static int[] stringToInts(String s)
    {
        String[] ss = s.split("\\|");
        int[] v = new int[ss.length];

        for (int i = 0; i < ss.length; i++)
            v[i] = Integer.valueOf(ss[i]);

        return v;
    }
    
    public static byte[] longsToBytes(long[] v)
    {
        byte[] b = new byte[v.length * 8];

        for (int i = 0 ; i < v.length ; ++i)
            for (int j = 0 ; j < 8 ; ++j)
                b[i*8+j] = (byte) (v[i] >> (7-j)*8);

        return b;
    }


    public static long[] bytesToLongs(byte[] b)
    {
        long[] v = new long[b.length / 8];

        for (int i = 0 ; i < v.length ; ++i) {
            v[i] = 0;
            for (int j = 0 ; j < 8 ; ++j)
                v[i] |= (b[i*8+j] & 0xFFL) << (7-j)*8;
        }

        return v;
    }

    public static byte[] doublesToBytes(double[] v)
    {
        long[] w = new long[v.length];

        for (int i = 0 ; i < v.length ; ++i)
            w[i] = Double.doubleToLongBits(v[i]);

        return longsToBytes(w);
    }

    public static double[] bytesToDoubles(byte[] b)
    {
        long[] v = bytesToLongs(b);
        double[] w = new double[v.length];

        for (int i = 0 ; i < v.length ; ++i) {
            w[i] = Double.longBitsToDouble(v[i]);
        }

        return w;
    }

    public static double edist( double[] a, double[] b ) {
        double ret = 0.0;
        for (int i = 0 ; i < a.length ; ++i) {
            double diff = a[i] - b[i];
            ret += diff * diff;
        }
        return Math.sqrt(ret);
    }

    public static double product( double[] a, double[] b )
    {
        double ret = 0.0;
        for (int i = 0 ; i < a.length ; ++i) {
            ret += a[i] * b[i];
        }
        return ret;
    }

    public static double sum( double[] a )
    {
        double ret = 0.0;
        for (int i = 0 ; i < a.length ; ++i) {
            ret += a[i];
        }
        return ret;
    }

    public static double[] add( double[] a, double c )
    {
        double[] ret = new double[a.length];
        for (int i = 0 ; i < a.length ; ++i) {
            ret[i] = a[i] + c;
        }
        return ret;
    }

    public static Double corrWithStats( double[] a, double[] b )
    {
        int l = a.length - 2;

        double mean_a = a[l];
        double mean_b = b[l];
        double stdev_a = a[l+1];
        double stdev_b = b[l+1];

        if (stdev_a < epsilon || stdev_b < epsilon)
            return null;

        double sum = 0.0;
        for (int i = 0 ; i < l ; ++i) {
            sum += (a[i] - mean_a) * (b[i] - mean_b);
        }
        return sum / (stdev_a * stdev_b * l);
    }

    public static String getBaseEvent( String compositeEventId )
    {
        int lagsep = compositeEventId.indexOf("@-");
        if ( lagsep != -1 )
            return compositeEventId.substring(0, lagsep);

        if (compositeEventId.endsWith("-"))
            return compositeEventId.substring(0, compositeEventId.length()-1);

        return compositeEventId;
    }

    public static boolean matchesTarget(String eventId, String targets, StringBuffer featuresBuff)
    {
        if (eventId.endsWith("-") || eventId.contains("@-"))
            return false;

        Pattern t_pattern = Pattern.compile(targets);
        Matcher t_matcher = t_pattern.matcher(eventId);

        if (!t_matcher.matches())
            return false;

        int pos = 0;
        while (pos < featuresBuff.length()) {
            int found = featuresBuff.indexOf("#", pos);
            if ( found == -1 || found == featuresBuff.length() - 1 )
                break;

            int gr = Integer.valueOf(featuresBuff.substring(found + 1, found + 2));
            String repl = gr <= t_matcher.groupCount() ? t_matcher.group(gr) : "";

            featuresBuff.replace(found, found + 2, repl);
            pos += repl.length() - 2;
        }
        return true;
    }

}
