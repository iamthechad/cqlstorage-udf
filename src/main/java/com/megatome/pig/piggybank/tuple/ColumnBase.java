package com.megatome.pig.piggybank.tuple;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class ColumnBase<T> extends EvalFunc<T> {
    @SuppressWarnings("unchecked")
    @Override
    public T exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1) {
            return null;
        }

        final Tuple tuple = (Tuple)input.get(0);
        return (T)tuple.get(1);
    }
}
