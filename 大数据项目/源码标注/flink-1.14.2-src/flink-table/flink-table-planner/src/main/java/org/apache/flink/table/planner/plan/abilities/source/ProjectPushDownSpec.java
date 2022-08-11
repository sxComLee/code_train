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

package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link SourceAbilitySpec} that can not only serialize/deserialize the projection
 * to/from JSON, but also can push the projection into a {@link SupportsProjectionPushDown}.
 */
@JsonTypeName("ProjectPushDown")
public class ProjectPushDownSpec extends SourceAbilitySpecBase {
    public static final String FIELD_NAME_PROJECTED_FIELDS = "projectedFields";

    @JsonProperty(FIELD_NAME_PROJECTED_FIELDS)
    private final int[][] projectedFields;

    @JsonCreator
    public ProjectPushDownSpec(
            @JsonProperty(FIELD_NAME_PROJECTED_FIELDS) int[][] projectedFields,
            @JsonProperty(FIELD_NAME_PRODUCED_TYPE) RowType producedType) {
        super(producedType);
        this.projectedFields = checkNotNull(projectedFields);
    }

    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {
        if (tableSource instanceof SupportsProjectionPushDown) {
            ((SupportsProjectionPushDown) tableSource).applyProjection(projectedFields);
        } else {
            throw new TableException(
                    String.format(
                            "%s does not support SupportsProjectionPushDown.",
                            tableSource.getClass().getName()));
        }
    }
}
