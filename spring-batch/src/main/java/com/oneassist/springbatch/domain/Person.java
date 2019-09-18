package com.oneassist.springbatch;

public class Person {
    private String firstName;

    public Person(String firstName) {
        this.firstName=firstName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    @Override
    public String toString() {
        return "Person{" +
                "firstName='" + firstName + '\'' +
                '}';
    }
}
