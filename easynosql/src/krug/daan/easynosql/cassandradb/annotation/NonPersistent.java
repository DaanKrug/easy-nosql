package krug.daan.easynosql.cassandradb.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author Daniel Augusto Krug
 * To annotate object attributes that are non persistent (or transients)
 * and should not be on CassandraDB tables.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NonPersistent {

}
