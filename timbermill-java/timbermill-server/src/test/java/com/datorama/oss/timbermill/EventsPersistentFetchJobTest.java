package com.datorama.oss.timbermill;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;

import com.datorama.oss.timbermill.cron.EventsPersistentFetchJob;
import com.datorama.oss.timbermill.pipe.LocalOutputPipe;
import com.datorama.oss.timbermill.unit.*;
import com.google.common.collect.Maps;

import static com.datorama.oss.timbermill.TimberLogTest.TEST;
import static com.datorama.oss.timbermill.TimberLogTest.assertTask;
import static com.datorama.oss.timbermill.common.ElasticsearchUtil.*;
import static org.junit.Assert.assertEquals;

public class EventsPersistentFetchJobTest {

	private static LocalOutputPipe pipe;
	private static ElasticsearchClient client;

	@BeforeClass
	public static void init()  {
		String elasticUrl = System.getenv("ELASTICSEARCH_URL");
		if (StringUtils.isEmpty(elasticUrl)){
			elasticUrl = "http://localhost:9200";
		}
		LocalOutputPipe.Builder builder = new LocalOutputPipe.Builder().diskHandlerStrategy("sqlite").url(elasticUrl).deletionCronExp("").
				bulkPersistentFetchCronExp("").eventsPersistentFetchCronExp("").mergingCronExp("");
		pipe = builder.build();

		client = new ElasticsearchClientForTests(elasticUrl, null);
		TimberLogger.bootstrap(pipe, TEST);
	}

	@AfterClass
	public static void tearDown(){
		TimberLogger.exit();
		pipe.close();
	}

	@Test
	public void testFullQueue() {
		String name = "full_queue_tests";
		String id = Event.generateTaskId(name);
		String ctx = "ctx";
		String str = "str";
		String metric = "metric";
		String text = "text";
		Event startEvent = new StartEvent(id, name, LogParams.create().context(ctx, ctx).string(str, str).metric(metric, 1).text(text, text), null);
		Event successEvent = new SuccessEvent(id, LogParams.create());
		startEvent.setEnv(TEST);
		successEvent.setEnv(TEST);

		pipe.getOverflowedQueue().add(startEvent);
		pipe.getOverflowedQueue().add(successEvent);

		EventsPersistentFetchJob job = new EventsPersistentFetchJob();
		JobExecutionContext context = new JobExecutionContextTest();
		try {
			Thread.sleep(5000);
			job.execute(context);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
		TimberLogTest.waitForTask(id, TaskStatus.SUCCESS, client);
		Task task = client.getTaskById(id);
		assertTask(task, name, true, true, id, null, TaskStatus.SUCCESS);
		assertEquals(ctx, task.getCtx().get(ctx));
		assertEquals(str, task.getString().get(str));
		assertEquals(1, task.getMetric().get(metric).intValue());
		assertEquals(text, task.getText().get(text));
	}

	private static class JobExecutionContextTest implements JobExecutionContext {
		@Override public Scheduler getScheduler() {
			return null;
		}

		@Override public Trigger getTrigger() {
			return null;
		}

		@Override public Calendar getCalendar() {
			return null;
		}

		@Override public boolean isRecovering() {
			return false;
		}

		@Override public TriggerKey getRecoveringTriggerKey() throws IllegalStateException {
			return null;
		}

		@Override public int getRefireCount() {
			return 0;
		}

		@Override public JobDataMap getMergedJobDataMap() {
			return null;
		}

		@Override public JobDetail getJobDetail() {
			JobDetailImpl jobDetail = new JobDetailImpl();
			Map<String, Object> map = Maps.newHashMap();
			map.put(DISK_HANDLER, pipe.getDiskHandler());
			map.put(EVENTS_QUEUE, pipe.getBuffer());
			map.put(OVERFLOWED_EVENTS_QUEUE, pipe.getOverflowedQueue());
			JobDataMap jobMap = new JobDataMap(map);
			jobDetail.setJobDataMap(jobMap);
			return jobDetail;
		}

		@Override public Job getJobInstance() {
			return null;
		}

		@Override public Date getFireTime() {
			return null;
		}

		@Override public Date getScheduledFireTime() {
			return null;
		}

		@Override public Date getPreviousFireTime() {
			return null;
		}

		@Override public Date getNextFireTime() {
			return null;
		}

		@Override public String getFireInstanceId() {
			return null;
		}

		@Override public Object getResult() {
			return null;
		}

		@Override public void setResult(Object result) {

		}

		@Override public long getJobRunTime() {
			return 0;
		}

		@Override public void put(Object key, Object value) {

		}

		@Override public Object get(Object key) {
			return null;
		}
	}
}