package com.megatome.pig.piggybank.tuple;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class LongColumnBase extends ColumnBase<Long> {
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input), DataType.LONG));
    }
}
