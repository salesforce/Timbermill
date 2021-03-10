package com.datorama.oss.timbermill;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.http.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;

import com.amazonaws.DefaultRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.Signer;
import com.amazonaws.http.HttpMethodName;


public class AWSRequestSigningApacheInterceptor implements HttpRequestInterceptor {
	private final String service;
	private final Signer signer;
	private final AWSCredentialsProvider awsCredentialsProvider;

	AWSRequestSigningApacheInterceptor(final String service,
			final Signer signer,
			final AWSCredentialsProvider awsCredentialsProvider) {
		this.service = service;
		this.signer = signer;
		this.awsCredentialsProvider = awsCredentialsProvider;
	}

	@Override
	public void process(final HttpRequest request, final HttpContext context) throws IOException {
		URIBuilder uriBuilder;
		try {
			uriBuilder = new URIBuilder(request.getRequestLine().getUri());
		} catch (URISyntaxException e) {
			throw new IOException("Invalid URI" , e);
		}
		DefaultRequest<?> signableRequest = new DefaultRequest<>(service);

		HttpHost host = (HttpHost) context.getAttribute(HttpCoreContext.HTTP_TARGET_HOST);
		if (host != null) {
			signableRequest.setEndpoint(URI.create(host.toURI()));
		}
		final HttpMethodName httpMethod =
				HttpMethodName.fromValue(request.getRequestLine().getMethod());
		signableRequest.setHttpMethod(httpMethod);
		try {
			signableRequest.setResourcePath(uriBuilder.build().getRawPath());
		} catch (URISyntaxException e) {
			throw new IOException("Invalid URI" , e);
		}

		if (request instanceof HttpEntityEnclosingRequest) {
			HttpEntityEnclosingRequest httpEntityEnclosingRequest =
					(HttpEntityEnclosingRequest) request;
			if (httpEntityEnclosingRequest.getEntity() == null) {
				signableRequest.setContent(new ByteArrayInputStream(new byte[0]));
			} else {
				signableRequest.setContent(httpEntityEnclosingRequest.getEntity().getContent());
			}
		}
		signableRequest.setParameters(nvpToMapParams(uriBuilder.getQueryParams()));
		signableRequest.setHeaders(headerArrayToMap(request.getAllHeaders()));

		signer.sign(signableRequest, awsCredentialsProvider.getCredentials());

		request.setHeaders(mapToHeaderArray(signableRequest.getHeaders()));
		if (request instanceof HttpEntityEnclosingRequest) {
			HttpEntityEnclosingRequest httpEntityEnclosingRequest =
					(HttpEntityEnclosingRequest) request;
			if (httpEntityEnclosingRequest.getEntity() != null) {
				BasicHttpEntity basicHttpEntity = new BasicHttpEntity();
				basicHttpEntity.setContent(signableRequest.getContent());
				httpEntityEnclosingRequest.setEntity(basicHttpEntity);
			}
		}
	}

	private static Map<String, List<String>> nvpToMapParams(final List<NameValuePair> params) {
		Map<String, List<String>> parameterMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		for (NameValuePair nvp : params) {
			List<String> argsList =
					parameterMap.computeIfAbsent(nvp.getName(), k -> new ArrayList<>());
			argsList.add(nvp.getValue());
		}
		return parameterMap;
	}

	private static Map<String, String> headerArrayToMap(final Header[] headers) {
		Map<String, String> headersMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
		for (Header header : headers) {
			if (!skipHeader(header)) {
				headersMap.put(header.getName(), header.getValue());
			}
		}
		return headersMap;
	}

	private static boolean skipHeader(final Header header) {
		return ("content-length".equalsIgnoreCase(header.getName())
				&& "0".equals(header.getValue()))
				|| "host".equalsIgnoreCase(header.getName());
	}

	private static Header[] mapToHeaderArray(final Map<String, String> mapHeaders) {
		Header[] headers = new Header[mapHeaders.size()];
		int i = 0;
		for (Map.Entry<String, String> headerEntry : mapHeaders.entrySet()) {
			headers[i++] = new BasicHeader(headerEntry.getKey(), headerEntry.getValue());
		}
		return headers;
	}
}