package br.com.anteros.nosql.persistence.mongodb.query;

import com.mongodb.BasicDBObject;


public class Projection {

	public static ProjectionBuilder include(String... fields) {
        return new ProjectionBuilder().include(fields);
    }

    
    public static ProjectionBuilder exclude(String... fields) {
        return new ProjectionBuilder().exclude(fields);
    }

    public static class ProjectionBuilder extends BasicDBObject {
        private ProjectionBuilder() {
        }

        
        public ProjectionBuilder include(String... fields) {
            for (String field : fields) {
                put(field, 1);
            }
            return this;
        }

        
        public ProjectionBuilder exclude(String... fields) {
            for (String field : fields) {
                put(field, 0);
            }
            return this;
        }
    }
}
