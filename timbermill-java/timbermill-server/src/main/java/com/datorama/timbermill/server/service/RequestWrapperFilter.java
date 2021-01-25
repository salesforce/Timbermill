package com.datorama.timbermill.server.service;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

import com.google.common.net.HttpHeaders;

import kamon.metric.Timer;
import static com.datorama.oss.timbermill.common.KamonConstants.GZIP_DECOMPRESS_REQUEST_DURATION;

@Component
public class RequestWrapperFilter extends OncePerRequestFilter {

	@Override
	protected void doFilterInternal(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse, FilterChain filterChain) throws ServletException, IOException {

		boolean isGzipped = httpServletRequest.getHeader(HttpHeaders.CONTENT_ENCODING) != null && httpServletRequest.getHeader(HttpHeaders.CONTENT_ENCODING).contains("gzip");
		if (isGzipped) {
			Timer.Started gzipTimer = GZIP_DECOMPRESS_REQUEST_DURATION.withoutTags().start();
			httpServletRequest = new GzippedInputStreamWrapper(httpServletRequest);
			gzipTimer.stop();

		} else {
			httpServletRequest = new ContentCachingRequestWrapper(httpServletRequest);
		}
		filterChain.doFilter(httpServletRequest, httpServletResponse);
	}
}
