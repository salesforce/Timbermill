package com.datorama.oss.timbermill.annotation;
import org.springframework.context.annotation.Configuration;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import com.datorama.oss.timbermill.TimberLogger;

@Aspect
public class TimberLogAspect {

    @Around("(execution(* *(..)) || execution(*.new(..))) && @annotation(timberLogAnnotation)")
    public Object timberLog(ProceedingJoinPoint pjp, TimberLogTask timberLogAnnotation) throws Throwable {
        TimberLogger.start(timberLogAnnotation.name());
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
