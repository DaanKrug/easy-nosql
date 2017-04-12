package krug.daan.easynosql.cassandradb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Daniel Augusto Krug
 *
 * To annotate the attributes of a object that should have a secondary index on
 * CassandraDB to be possible search in CQL (Cassandra Query Language).
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface SecondaryIndex {

}
