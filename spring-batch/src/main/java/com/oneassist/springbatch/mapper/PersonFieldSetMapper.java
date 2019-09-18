package com.oneassist.springbatch;

import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class PersonFieldSetMapper implements org.springframework.batch.item.file.mapping.FieldSetMapper<Person> {
    @Override
    public Person mapFieldSet(FieldSet fieldSet) throws BindException {
        String firstName = fieldSet.readString(0);

        return new Person(firstName);
    }
}
