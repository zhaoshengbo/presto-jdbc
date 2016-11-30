/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.client;

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Base64;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.*;

import static com.facebook.presto.spi.type.StandardTypes.*;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.utils.Objects.requireNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Collections.unmodifiableList;

@Immutable
public class QueryResults {
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Column> columns;
    private final Iterable<List<Object>> data;
    private final StatementStats stats;
    private final QueryError error;
    private final String updateType;
    private final Long updateCount;

    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount) {
        this(id, infoUri, partialCancelUri, nextUri, columns, fixData(columns, data), stats, error, updateType, updateCount);
    }

    public QueryResults(
            String id,
            URI infoUri,
            URI partialCancelUri,
            URI nextUri,
            List<Column> columns,
            Iterable<List<Object>> data,
            StatementStats stats,
            QueryError error,
            String updateType,
            Long updateCount) {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    @NotNull
    @JsonProperty
    public String getId() {
        return id;
    }

    @NotNull
    @JsonProperty
    public URI getInfoUri() {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    public URI getPartialCancelUri() {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri() {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    public List<Column> getColumns() {
        return columns;
    }

    @Nullable
    @JsonProperty
    public Iterable<List<Object>> getData() {
        return data;
    }

    @NotNull
    @JsonProperty
    public StatementStats getStats() {
        return stats;
    }

    @Nullable
    @JsonProperty
    public QueryError getError() {
        return error;
    }

    @Nullable
    @JsonProperty
    public String getUpdateType() {
        return updateType;
    }

    @Nullable
    @JsonProperty
    public Long getUpdateCount() {
        return updateCount;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("nextUri", nextUri)
                .add("columns", columns)
                .add("hasData", data != null)
                .add("stats", stats)
                .add("error", error)
                .add("updateType", updateType)
                .add("updateCount", updateCount)
                .toString();
    }

    private static Iterable<List<Object>> fixData(List<Column> columns, List<List<Object>> data) {
        if (data == null) {
            return null;
        }
        requireNonNull(columns, "columns is null");
        List<TypeSignature> signatures = new ArrayList<TypeSignature>();
        for (Column column : columns) {
            signatures.add(parseTypeSignature(column.getType()));
        }
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (List<Object> row : data) {
            checkArgument(row.size() == columns.size(), "row/column size mismatch");
            List<Object> newRow = new ArrayList<Object>();
            for (int i = 0; i < row.size(); i++) {
                newRow.add(fixValue(signatures.get(i), row.get(i)));
            }
            rows.add(unmodifiableList(newRow)); // allow nulls in list
        }
        return rows.build();
    }

    /**
     * Force values coming from Jackson to have the expected object type.
     */
    private static Object fixValue(TypeSignature signature, Object value) {
        if (value == null) {
            return null;
        }

        if (signature.getBase().equals(ARRAY)) {
            List<Object> fixedValue = new ArrayList<Object>();
            for (Object object : List.class.cast(value)) {
                fixedValue.add(fixValue(signature.getTypeParametersAsTypeSignatures().get(0), object));
            }
            return fixedValue;
        }
        if (signature.getBase().equals(MAP)) {
            TypeSignature keySignature = signature.getTypeParametersAsTypeSignatures().get(0);
            TypeSignature valueSignature = signature.getTypeParametersAsTypeSignatures().get(1);
            Map<Object, Object> fixedValue = new HashMap<Object, Object>();
            for (Map.Entry<?, ?> entry : (Set<Map.Entry<?, ?>>) Map.class.cast(value).entrySet()) {
                fixedValue.put(fixValue(keySignature, entry.getKey()), fixValue(valueSignature, entry.getValue()));
            }
            return fixedValue;
        }
        if (signature.getBase().equals(ROW)) {
            Map<String, Object> fixedValue = new LinkedHashMap<String, Object>();
            List<Object> listValue = List.class.cast(value);
            checkArgument(listValue.size() == signature.getParameters().size(), "Mismatched data values and row type");
            for (int i = 0; i < listValue.size(); i++) {
                TypeSignatureParameter parameter = signature.getParameters().get(i);
                checkArgument(
                        parameter.getKind() == ParameterKind.NAMED_TYPE,
                        "Unexpected parameter [%s] for row type",
                        parameter);
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                String key = namedTypeSignature.getName();
                fixedValue.put(key, fixValue(namedTypeSignature.getTypeSignature(), listValue.get(i)));
            }
            return fixedValue;
        }
        String signatureBase = signature.getBase();
        if (BIGINT.equals(signatureBase)) {
            if (value instanceof String) {
                return Long.parseLong((String) value);
            }
            return ((Number) value).longValue();
        } else if (INTEGER.equals(signatureBase)) {
            if (value instanceof String) {
                return Integer.parseInt((String) value);
            }
            return ((Number) value).intValue();
        } else if (SMALLINT.equals(signatureBase)) {
            if (value instanceof String) {
                return Short.parseShort((String) value);
            }
            return ((Number) value).shortValue();
        } else if (TINYINT.equals(signatureBase)) {
            if (value instanceof String) {
                return Byte.parseByte((String) value);
            }
            return ((Number) value).byteValue();
        } else if (DOUBLE.equals(signatureBase)) {
            if (value instanceof String) {
                return Double.parseDouble((String) value);
            }
            return ((Number) value).doubleValue();
        } else if (REAL.equals(signatureBase)) {
            if (value instanceof String) {
                return Float.parseFloat((String) value);
            }
            return ((Number) value).floatValue();
        } else if (BOOLEAN.equals(signatureBase)) {
            if (value instanceof String) {
                return Boolean.parseBoolean((String) value);
            }
            return Boolean.class.cast(value);
        } else if (VARCHAR.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (JSON.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (TIME.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (TIME_WITH_TIME_ZONE.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (TIMESTAMP.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (TIMESTAMP_WITH_TIME_ZONE.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (DATE.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (INTERVAL_YEAR_TO_MONTH.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (INTERVAL_DAY_TO_SECOND.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (DECIMAL.equals(signatureBase)) {
            return String.class.cast(value);
        } else if (CHAR.equals(signatureBase)) {
            return String.class.cast(value);
        } else {
            // for now we assume that only the explicit types above are passed
            // as a plain text and everything else is base64 encoded binary
            if (value instanceof String) {

                return Base64.decodeBase64((String) value);
            }
            return value;
        }
    }
}
