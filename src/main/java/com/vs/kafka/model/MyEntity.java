package com.vs.kafka.model;

import java.util.Objects;

public class MyEntity {

    public final Integer intValue;
    public final String stringValue;

    public MyEntity(Integer intValue, String stringValue) {
        this.intValue = intValue;
        this.stringValue = stringValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MyEntity myEntity = (MyEntity) o;
        return Objects.equals(intValue, myEntity.intValue) &&
                Objects.equals(stringValue, myEntity.stringValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(intValue, stringValue);
    }
}
