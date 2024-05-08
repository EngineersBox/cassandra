/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.metrics.serde;

import com.google.common.base.CaseFormat;

public enum SerializerType
{
    // Time for unfiltered serializer to serialize the entire row
    ROW,
    // Time for unfiltered serializer to serialize the row body
    ROW_BODY,
    // Time for unfiltered serializer to serialize the current
    // column of this row
    COLUMN,
    // Time for unfiltered serializer to serialize all the
    // columns for this row
    ALL_COLUMNS,
    // Time for unfiltered serialize to serialize header columns
    // for this row when the HAS_ALL_COLUMNS flag is set
    COLUMN_SUBSET,
    // Time for unfiltered serializer to serialize a range tombstone marker
    // in this row
    RANGE_TOMBSTONE_MARKER,
    // Time for clustering prefix to serialize the clustering key
    CLUSTERING_KEY,
    // Time for cell to serialize the entire cell
    CELL,
    // Time for column family store to serialize and flush an SSTable to disk
    CFS,
    // Time taken to serialize an appended index entry (decorated key + abstract index entry)
    // within a table writer (BigTableWriter, BtiTableWriter)
    INDEX_ENTRY,
    // Time taken from creation of partition writer to finish() invocation
    PARTITION;

    public String metricName()
    {
        return CaseFormat.UPPER_UNDERSCORE.to(
        CaseFormat.UPPER_CAMEL,
        name()
        );
    }
}
