package com.datorama.oss.timbermill.annotation;

import com.datorama.oss.timbermill.TimberLogger;
import com.datorama.oss.timbermill.unit.LogParams;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import org.aspectj.lang.reflect.CodeSignature;

@Aspect
public class TimberLogAspect {

    @Around("(execution(* *(..)) || execution(*.new(..))) && @annotation(timberLogAnnotation)")
    public Object timberLog(ProceedingJoinPoint pjp, TimberLogTask timberLogAnnotation) throws Throwable {
        LogParams logParams = null;
        if (timberLogAnnotation.logParameters()) {
            logParams = getLogParamsForMethodParams(pjp, logParams);
        }
        String taskId = TimberLogger.start(timberLogAnnotation.name(), logParams);
        try {
            Object obj = pjp.proceed();
            TimberLogger.success(taskId);
            return obj;
        } catch (Exception e){
            TimberLogger.error(taskId, e);
            throw e;
        }
    }

    private LogParams getLogParamsForMethodParams(ProceedingJoinPoint pjp, LogParams logParams) {
        Signature signature = pjp.getSignature();
        if (signature instanceof CodeSignature) {
            CodeSignature codeSignature = (CodeSignature) signature;
            String[] parameterNames = codeSignature.getParameterNames();
            Object[] parameterValues = pjp.getArgs();

            if (parameterNames != null && parameterValues != null && parameterNames.length == parameterValues.length) {
                logParams = LogParams.create();
                for (int i = 0; i < parameterNames.length; i++) {
                    logParams.string(parameterNames[i], String.valueOf(parameterValues[i]));
                }
            }
        }
        return logParams;
    }
}
