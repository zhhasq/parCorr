
/*=============================================================================
  Copyright (c) 2020, INRIA, France

  Distributed under the MIT License (See accompanying file LICENSE).

  The research leading to this code has been partially funded by the
  European Commission under Horizon 2020 programme project #732051.
=============================================================================*/

package fr.inria.zenith.cep;

import es.upm.cep.commons.exception.CepOperatorException;

public interface IWindowCallback {
    void handleTimestampAdvance( long oldTS, long newTS, int eventsProcessed, int eventsDiscarded, long timeMs ) throws CepOperatorException;
}
