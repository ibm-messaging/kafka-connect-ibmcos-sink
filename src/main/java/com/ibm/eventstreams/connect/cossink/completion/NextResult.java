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

/**
 * The result from testing whether adding a subsequent {@code SinkRecord}
 * to an object will complete the object.
 */
public enum NextResult {

    /**
     * The object is complete, including this record as the last record
     * that makes up the object.
     */
    COMPLETE_INCLUSIVE,

    /**
     * The object is complete and includes all records up to (but not
     * including) this record.
     */
    COMPLETE_NON_INCLUSIVE,

    /**
     * The object is not complete.
     */
    INCOMPLETE
}
