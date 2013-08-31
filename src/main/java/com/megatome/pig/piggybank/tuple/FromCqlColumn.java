/*
 * Copyright 2013 Megatome Technologies, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */

package com.megatome.pig.piggybank.tuple;

import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.ArrayList;
import java.util.List;

public class FromCqlColumn extends StringColumnBase {
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        final List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        funcList.add(new FuncSpec(this.getClass().getName(), makeSchema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
        funcList.add(new FuncSpec(DoubleColumnBase.class.getName(), makeSchema(new Schema.FieldSchema(null, DataType.DOUBLE))));
        funcList.add(new FuncSpec(FloatColumnBase.class.getName(), makeSchema(new Schema.FieldSchema(null, DataType.FLOAT))));
        funcList.add(new FuncSpec(IntColumnBase.class.getName(), makeSchema(new Schema.FieldSchema(null, DataType.INTEGER))));
        funcList.add(new FuncSpec(LongColumnBase.class.getName(), makeSchema(new Schema.FieldSchema(null, DataType.LONG))));
        return funcList;
    }

    private Schema makeSchema(final Schema.FieldSchema fieldSchema) {
        final Schema schema = new Schema();
        schema.add(fieldSchema);
        return schema;
    }
}
