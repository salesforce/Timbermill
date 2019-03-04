# Timbermill

### Task-Based,A Context-Aware Logging service

Timbermill is an advanced, open-source logging service built for [Elasticsearch](https://www.elastic.co/products/elasticsearch).  
Timbermill collects all events sent to it, processes them, and sends them to your elasticsearch cluster in a **task-based, context-aware** manner which gives an easy way to search, analyze and monitor your system. (mainly by using Kibana's advantages)

#### Task-based, Context-Aware, cool. What's that?

##### Task-based:
Timbermill logs aren't just lines of text, they are `Tasks`.


`Task` characteristics:
  * Represented by a specific `Type` (not to be confused with Elasticsearch document's type).
  * Has a unique ID.
  * Represents multiple `Events` that where called for it.
    * Start event that open the Task.
    * Zero or more info events (`Attributes`/`Data`/`Metrics`) that add properties values to the task. 
    * Closing event that closes the Task either successfully or with an error (`Success`/`Error`), note that as long as an event is in the works its status is `Unterminated`
  * Has a start-time, end-time and duration. 

##### Context-Aware: 
System events doesn't occur in a void.
 
Events always have context.  When did this event happen? Why did it happen? What was the path of events leading to this event? What were the values we encountered along the path?
It's rarely enough just to know that something happened and simple logging just doesn't let us know a lot more than that.

Timbermill does.

Every task in Timbermill points to a parent task and automatically keeps important data from it:
* Complete path of tasks leading to this task.
* Important properties from its ancestors.
* More!
* [Eden] please add here the default attributes that every event contains (e.g. host, jvm, ip etc.)


### Getting Started

#### Requirements
* Elasticsearch cluster
* Java project (1.8+)
* Add dependency to maven (hosted on Maven Central)


         <dependency>
             <groupId>com.datorama</groupId>
             <artifactId>timbermill-client</artifactId>
             <version>2.0.0</version>
         </dependency>


#### How to use Timbermill

Timbermill is designed to be plug-and-play out-of-the-box.  
 
```
TimberLog.bootstrap();
//...
try {
    TimberLog.start("hello_world");
    TimberLog.logAttributes("foo", "bar");
    TimberLog.logData("text", "This is a text!");
    TimberLog.logMetrics("number", 42);
    /**
     *  Your Code
     */
    TimberLog.success();
} catch (Exception e){
    TimberLog.error(e);
    throw e;
}
//...
TimberLog.exit();
```
                 
 This code bootstraps Timbermill with a local default Elasticsearch cluster (http://localhost:9200). It will write one task of type `hello_world` with the above properties to elasticsearch.
 
 ![Alt text](helloworld1.png?raw=true "Title")
 
 * The catch clouse is mandatory. If your code will throw an exception without a closing TimberLog method (success/error) being
called, tasks in Timbermill could become corrupted.
* A custom Elasticsearch URL (along with other [configurations](timbermill.configurations)) can be defined using `LocalOutputPipeConfig`.

```
LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().url("https://elasticsearch:9200");
LocalOutputPipeConfig config = builder.build();
TimberLog.bootstrap(config);
```

#### What can I do next?
 
 Get familiar with our [wiki](timbermill.wiki) so you get a better sense on how to properlly use Timbermill.
 
 
 ###### Things to add to the wiki
 * How timbermill works with different threads
 * Attributes vs. data vs. metrics
 * Explain all fields in task
 * Explain metadata on task
 * Explain all properties for timbermill bootstrap
 
