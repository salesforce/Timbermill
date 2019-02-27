package com.datorama.timbermill.annotation;

import com.datorama.timbermill.TimberLog;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

@Aspect
public class LogEventInterceptor {

	@Around("@annotation(logTaskAnnotation)")
	public Object logEvent(ProceedingJoinPoint joinPoint, LogTask logTaskAnnotation) throws Throwable {

		TimberLog.start(logTaskAnnotation.taskType());

		Object res;
		try {
			res = joinPoint.proceed();
		} catch (Throwable t) {
			TimberLog.error(t);
			throw t;
		}

		TimberLog.success();
		return res;
	}
}
