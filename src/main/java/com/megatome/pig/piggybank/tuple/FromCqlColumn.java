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
