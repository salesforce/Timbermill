package com.datorama.timbermill.annotation;

import java.lang.annotation.*;

@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Inherited
@Documented
public @interface LogTask {

	String taskType() default "";

}
