/*
 * Copyright 2010 Proofpoint, Inc.
 *
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
package io.airlift.json;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ObjectMapperProvider
{

    public ObjectMapperProvider()
    {

    }

    public ObjectMapper get()
    {
        ObjectMapper objectMapper = new ObjectMapper();

        // ignore unknown fields (for backwards compatibility)
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // do not allow converting a float to an integer
        objectMapper.disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT);

        // use ISO dates
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // skip fields that are null instead of writing an explicit json null value
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // disable auto detection of json properties... all properties must be explicit
        objectMapper.disable(MapperFeature.AUTO_DETECT_CREATORS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_FIELDS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_SETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_GETTERS);
        objectMapper.disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        objectMapper.disable(MapperFeature.USE_GETTERS_AS_SETTERS);
        objectMapper.disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS);
        objectMapper.disable(MapperFeature.INFER_PROPERTY_MUTATORS);
        objectMapper.disable(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS);

        return objectMapper;
    }

}
