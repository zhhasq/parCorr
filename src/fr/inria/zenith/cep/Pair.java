
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

public class Pair<F, S> {
    private F first;
    private S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public void setFirst(F first) {
        this.first = first;
    }

    public void setSecond(S second) {
        this.second = second;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    private static final int polyhashBase = 443;

    public int hashCode() {
        return first.hashCode() * polyhashBase + second.hashCode();
    }

    public boolean equals( Object o ) {
        Pair that = (Pair) o;

        return this.first.equals(that.first) && this.second.equals(that.second);
    }
}
