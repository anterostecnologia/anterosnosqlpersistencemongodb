package br.com.anteros.nosql.persistence.mongodb.geo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MultiPoint implements Geometry {
    private final List<Point> coordinates;

    @SuppressWarnings("UnusedDeclaration") 
    private MultiPoint() {
        this.coordinates = new ArrayList<Point>();
    }

    MultiPoint(final Point... points) {
        this.coordinates = Arrays.asList(points);
    }

    MultiPoint(final List<Point> coordinates) {
        this.coordinates = coordinates;
    }

    @Override
    public List<Point> getCoordinates() {
        return coordinates;
    }

    @Override
    public int hashCode() {
        return coordinates.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MultiPoint that = (MultiPoint) o;

        if (!coordinates.equals(that.coordinates)) {
            return false;
        }

        return true;
    }

    @Override
    public String toString() {
        return "MultiPoint{"
               + "coordinates=" + coordinates
               + '}';
    }
}
