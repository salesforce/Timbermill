package com.datorama.timbermill.pipe.redis.conf;

public class JedisClientConfig {

	//region members
	// pool properties
	private Integer maxTotalConnections; // max number of connections that can be created at a given time.
	private Integer maxIdleConnections; //This is the max number of connections that can be idle in the pool without being immediately evicted (closed)
	private Integer minIdleConnections; //This is the number of "warm" connections (e.g. ready for immediate use) that remain in the pool even when load has reduced. If not set, the default is 0.
	private boolean testOnReturn;
	private boolean testOnBorrow;
	private boolean testOnCreate;
	private boolean useSSL;
	// client properties
	private String host; //e.g. localhost
	private Integer port;
	private Integer connectTimeout;
	private String password;
	//purge threads
	private Integer purgeThreadPoolSize = 10;

	private String evictionPolicyClassName;
	private Long minEvictableIdleTimeMillis;
	private Long softMinEvictableIdleTimeMillis;
	private Long timeBetweenEvictionRunsMillis;
	private Integer numTestsPerEvictionRun;
	private boolean testWhileIdle;
	//endregion

	//region constructors
	public JedisClientConfig(Integer maxTotalConnections, Integer maxIdleConnections, Integer minIdleConnections, boolean testOnReturn, boolean testOnBorrow, boolean testOnCreate, String host,
			Integer port,
			Integer connectTimeout,
			String password,
			boolean useSSL,
			String evictionPolicyClassName,
			Long minEvictableIdleTimeMillis,
			Long softMinEvictableIdleTimeMillis,
			Long timeBetweenEvictionRunsMillis,
			Integer numTestsPerEvictionRun,
			boolean testWhileIdle) {
		this.maxTotalConnections = maxTotalConnections;
		this.maxIdleConnections = maxIdleConnections;
		this.minIdleConnections = minIdleConnections;
		this.testOnReturn = testOnReturn;
		this.testOnBorrow = testOnBorrow;
		this.testOnCreate = testOnCreate;
		this.useSSL = useSSL;
		this.host = host;
		this.port = port;
		this.connectTimeout = connectTimeout;
		this.password = password;
		this.evictionPolicyClassName = evictionPolicyClassName;
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
		this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
		this.testWhileIdle = testWhileIdle;
	}
	//endregion

	//region getters and setters
	public Integer getMaxTotalConnections() {
		return maxTotalConnections;
	}

	void setMaxTotalConnections(Integer maxTotalConnections) {
		this.maxTotalConnections = maxTotalConnections;
	}

	public Integer getMaxIdleConnections() {
		return maxIdleConnections;
	}

	void setMaxIdleConnections(Integer maxIdleConnections) {
		this.maxIdleConnections = maxIdleConnections;
	}

	public Integer getMinIdleConnections() {
		return minIdleConnections;
	}

	void setMinIdleConnections(Integer minIdleConnections) {
		this.minIdleConnections = minIdleConnections;
	}

	public String getHost() {
		return host;
	}

	void setHost(String host) {
		this.host = host;
	}

	public Integer getPort() {
		return port;
	}

	void setPort(Integer port) {
		this.port = port;
	}

	public Integer getConnectTimeout() {
		return connectTimeout;
	}

	void setConnectTimeout(Integer connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public String getPassword() {
		return password;
	}

	void setPassword(String password) {
		this.password = password;
	}

	Integer getPurgeThreadPoolSize() {
		return purgeThreadPoolSize;
	}

	void setPurgeThreadPoolSize(Integer purgeThreadPoolSize) {
		this.purgeThreadPoolSize = purgeThreadPoolSize;
	}

	public boolean isTestOnReturn() {
		return testOnReturn;
	}

	void setTestOnReturn(boolean testOnReturn) {
		this.testOnReturn = testOnReturn;
	}

	public boolean isTestOnBorrow() {
		return testOnBorrow;
	}

	void setTestOnBorrow(boolean testOnBorrow) {
		this.testOnBorrow = testOnBorrow;
	}

	public boolean isTestOnCreate() {
		return testOnCreate;
	}

	void setTestOnCreate(boolean testOnCreate) {
		this.testOnCreate = testOnCreate;
	}

	public boolean isUseSSL() {
		return useSSL;
	}

	void setUseSSL(boolean useSSL) {
		this.useSSL = useSSL;
	}

	public void setEvictionPolicyClassName(String evictionPolicyClassName) {
		this.evictionPolicyClassName = evictionPolicyClassName;
	}

	public String getEvictionPolicyClassName() {
		return evictionPolicyClassName;
	}

	public void setMinEvictableIdleTimeMillis(Long minEvictableIdleTimeMillis) {
		this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
	}

	public Long getMinEvictableIdleTimeMillis() {
		return minEvictableIdleTimeMillis;
	}

	public void setSoftMinEvictableIdleTimeMillis(Long softMinEvictableIdleTimeMillis) {
		this.softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
	}

	public Long getSoftMinEvictableIdleTimeMillis() {
		return softMinEvictableIdleTimeMillis;
	}

	public void setTimeBetweenEvictionRunsMillis(Long timeBetweenEvictionRunsMillis) {
		this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
	}

	public Long getTimeBetweenEvictionRunsMillis() {
		return timeBetweenEvictionRunsMillis;
	}

	public void setNumTestsPerEvictionRun(Integer numTestsPerEvictionRun) {
		this.numTestsPerEvictionRun = numTestsPerEvictionRun;
	}

	public Integer getNumTestsPerEvictionRun() {
		return numTestsPerEvictionRun;
	}

	public void setTestWhileIdle(boolean testWhileIdle) {
		this.testWhileIdle = testWhileIdle;
	}

	public boolean getTestWhileIdle() {
		return testWhileIdle;
	}

	//endregion
}