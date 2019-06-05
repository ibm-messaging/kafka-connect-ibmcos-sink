/*
 * Copyright 2019 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.eventstreams.connect.cossink.completion;

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;

/**
 * CompletionCriteriaSet manages a set of {@code ObjectCompletionCriteria}
 * instances. It simplifies propagating the life-cycle events to each
 * instance.
 */
public class CompletionCriteriaSet {

    private final Set<ObjectCompletionCriteria> criteriaSet = new HashSet<>();

    public CompletionCriteriaSet() {}

    public void add(ObjectCompletionCriteria criteria) {
        criteriaSet.add(criteria);
    }

    public boolean isEmpty() {
        return criteriaSet.isEmpty();
    }

    public FirstResult first(SinkRecord sinkRecord, AsyncCompleter asyncCompleter) {
        FirstResult result = FirstResult.INCOMPLETE;
        for (ObjectCompletionCriteria criteria : criteriaSet) {
            if (criteria.first(sinkRecord, asyncCompleter) == FirstResult.COMPLETE) {
                result = FirstResult.COMPLETE;
            }
        }
        return result;
    }

    /**
     * Propagates the call to each implementation's {@code ObjectCompletionCriteria#next(SinkRecord)}
     * method.
     *
     * @param sinkRecord
     *
     * @return the aggregate of all of the {@code Result} values returned by each implementation.
     *           if any of the instances return {@code Result#COMPLETE_NON_INCLUSIVE} then this
     *           is returned. Otherwise if any of the instances return
     *           {@code Result#COMPLETE_INCLUSIVE} then this is returned. Otherwise the default
     *           is to return {@code Result#INCOMPLETE}.
     */
    public NextResult next(SinkRecord sinkRecord) {
      NextResult result = NextResult.INCOMPLETE;
      for (ObjectCompletionCriteria criteria : criteriaSet) {
          switch (criteria.next(sinkRecord)) {
          case COMPLETE_INCLUSIVE:
              if (result == NextResult.INCOMPLETE) {
                  result = NextResult.COMPLETE_INCLUSIVE;
              }
              break;
          case COMPLETE_NON_INCLUSIVE:
              if (result == NextResult.COMPLETE_INCLUSIVE || result == NextResult.INCOMPLETE) {
                  result = NextResult.COMPLETE_NON_INCLUSIVE;
              }
              break;
          case INCOMPLETE:
              break;
          }
      }
      return result;
    }

    public void complete() {
        for (ObjectCompletionCriteria criteria : criteriaSet) {
            criteria.complete();
        }
    }
}
