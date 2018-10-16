package br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.utils;

import java.util.List;

import br.com.anteros.nosql.persistence.mongodb.query.rsql.visitor.structs.Lazy;

public final class LazyUtils {
    private LazyUtils(){}


    public static <T> T firstThatReturnsNonNull(List<Lazy<T>> lazies) {
        for(Lazy<T> lazy : lazies) {
            T val = lazy.get();
            if(val != null) {
                return val;
            }
        }
        return null;
    }


}
