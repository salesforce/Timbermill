package com.datorama.timbermill.server.service;

import java.io.*;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.WebUtils;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.io.ByteStreams;
import com.google.common.net.HttpHeaders;

/**
 * Wrapper class that detects if the request is gzipped and ungzipps it.
 */
final class GzippedInputStreamWrapper extends HttpServletRequestWrapper {

	//private static final Logger LOG = LoggerFactory.getLogger(GzippedInputStreamWrapper.class);
	private static final Logger LOG = LoggerFactory.getLogger(GzippedInputStreamWrapper.class);


	/**
	 * Default encoding that is used when post parameters are parsed.
	 */
	static final String DEFAULT_ENCODING = WebUtils.DEFAULT_CHARACTER_ENCODING;

	/**
	 * Serialized bytes array that is a result of unzipping gzipped body.
	 */
	private byte[] bytes;

	/**
	 * Constructs a request object wrapping the given request.
	 * In case if Content-Encoding contains "gzip" we wrap the input stream into byte array
	 * to original input stream has nothing in it but hew wrapped input stream always returns
	 * reproducible ungzipped input stream.
	 *
	 * @param request request which input stream will be wrapped.
	 * @throws java.io.IOException when input stream reqtieval failed.
	 */
	GzippedInputStreamWrapper(final HttpServletRequest request) throws IOException {
		super(request);
		try {
			final InputStream in = new GZIPInputStream(request.getInputStream());
			bytes = ByteStreams.toByteArray(in);
		} catch (EOFException e) {
			bytes = new byte[0];
		}
	}


	/**
	 * @return reproduceable input stream that is either equal to initial servlet input
	 * stream(if it was not zipped) or returns unzipped input stream.
	 */
	@Override public ServletInputStream getInputStream() {
		final ByteArrayInputStream sourceStream = new ByteArrayInputStream(bytes);
		return new ServletInputStream() {
			private ReadListener readListener;

			@Override public boolean isFinished() {
				return sourceStream.available() <= 0;
			}

			@Override public boolean isReady() {
				return sourceStream.available() > 0;
			}

			@Override public void setReadListener(ReadListener readListener) {
				this.readListener = readListener;
			}

			public ReadListener getReadListener() {
				return readListener;
			}

			public int read() {
				return sourceStream.read();
			}

			@Override
			public void close() throws IOException {
				super.close();
				sourceStream.close();
			}
		};
	}

	/**
	 * Need to override getParametersMap because we initially read the whole input stream and
	 * servlet container won't have access to the input stream data.
	 *
	 * @return parsed parameters list. Parameters get parsed only when Content-Type
	 * "application/x-www-form-urlencoded" is set.
	 */
	@Override public Map<String, String[]> getParameterMap() {
		String contentEncodingHeader = getHeader(HttpHeaders.CONTENT_TYPE);
		if (!Strings.isNullOrEmpty(contentEncodingHeader) && contentEncodingHeader.contains("application/x-www-form-urlencoded")) {
			Map<String, String[]> params = new HashMap<>(super.getParameterMap());
			try {
				params.putAll(parseParams(new String(bytes)));
			} catch (UnsupportedEncodingException e) {
				LOG.error("Could not decompress incoming message!", e);
			}
			return params;
		} else {
			return super.getParameterMap();
		}
	}

	@Override
	public String getCharacterEncoding() {
		String enc = super.getCharacterEncoding();
		return (enc != null ? enc : DEFAULT_ENCODING);
	}

	/**
	 * parses params from the byte input stream.
	 *
	 * @param body request body serialized to string.
	 * @return parsed parameters map.
	 * @throws UnsupportedEncodingException if encoding provided is not supported.
	 */
	private Map<String, String[]> parseParams(final String body) throws UnsupportedEncodingException {
		String characterEncoding = getCharacterEncoding();
		final Multimap<String, String> parameters = ArrayListMultimap.create();
		for (String pair : body.split("&")) {
			if (Strings.isNullOrEmpty(pair)) {
				continue;
			}
			int idx = pair.indexOf('=');

			String key;
			if (idx > 0) {
				key = URLDecoder.decode(pair.substring(0, idx), characterEncoding);
			} else {
				key = pair;
			}
			String value;
			if (idx > 0 && pair.length() > idx + 1) {
				value = URLDecoder.decode(pair.substring(idx + 1), characterEncoding);
			} else {
				value = null;
			}
			parameters.put(key, value);
		}

		return parameters.asMap().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, kv -> Iterables.toArray(kv.getValue(), String.class)));
	}
}
