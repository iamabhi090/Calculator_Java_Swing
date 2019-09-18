package com.oneassist.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class PersonProcessor implements ItemProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonProcessor.class);
    @Override
    public Object process(Object o) throws Exception {
        Person p= (Person) o;
        String firstName = p.getFirstName().toUpperCase();

        Person processedPerson = new Person(firstName);

        LOGGER.info("Processed: " + p + " into: " + processedPerson);

        return processedPerson;
    }
}
