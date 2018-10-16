package br.com.anteros.nosql.persistence.mongodb.geo;

import java.util.List;

interface GeometryFactory {
    Geometry createGeometry(List<?> geometries);
}
