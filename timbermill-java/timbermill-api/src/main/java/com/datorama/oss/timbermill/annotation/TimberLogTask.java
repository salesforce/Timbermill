package com.datorama.oss.timbermill.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface TimberLogTask {
    String name();
    boolean logParameters() default false;
}
