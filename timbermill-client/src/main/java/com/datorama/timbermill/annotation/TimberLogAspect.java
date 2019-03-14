package com.datorama.timbermill.annotation;

import com.datorama.timbermill.TimberLogger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class TimberLogAspect {

    @Around("execution(* *(..)) && @annotation(timberLogAnnotation)")
    public Object timberLog(ProceedingJoinPoint pjp, TimberLog timberLogAnnotation) throws Throwable {
        TimberLogger.start(timberLogAnnotation.taskType());
        try {
            Object obj = pjp.proceed();
            TimberLogger.success();
            return obj;
        } catch (Exception e){
            TimberLogger.error(e);
            throw e;
        }
    }
}
