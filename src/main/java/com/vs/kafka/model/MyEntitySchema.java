package com.vs.kafka.model;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class MyEntitySchema {

    private static final String INT_FIELD = "intField";
    private static final String STRING_FIELD = "stringField";

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .name(MyEntitySchema.class.getSimpleName())
            .field(INT_FIELD, Schema.INT32_SCHEMA)
            .field(STRING_FIELD, Schema.STRING_SCHEMA)
            .build();


    public static Schema myEntityVectorSchema() {
        return SCHEMA;
    }

    public static Struct toStruct(MyEntity myEntity) {
        return new Struct(myEntityVectorSchema())
                .put(INT_FIELD, myEntity.intValue)
                .put(STRING_FIELD, myEntity.stringValue);
    }

    public static MyEntity fromStruct(Struct struct) {
        return new MyEntity(struct.getInt32(INT_FIELD), struct.getString(STRING_FIELD));
    }
}
