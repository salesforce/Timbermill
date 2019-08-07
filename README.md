# Timbermill

### A Task-Based, Context-Aware Logging service

Timbermill is an advanced, open-source logging service built for [Elasticsearch](https://www.elastic.co/products/elasticsearch).  
Timbermill collects all events sent to it, processes them, and sends them to your elasticsearch cluster in a **task-based, context-aware** manner which gives an easy way to search, analyze and monitor your system. (mainly by using Kibana)

#### Task-based, Context-Aware, cool. What's that?

##### Task-based:
Timbermill logs aren't just lines of text, they are `Tasks`.


`Task` characteristics:
  * Represented by a specific `name`.
  * Has a unique ID.
  * Represents multiple `Events` that where called for it.
    * Start event that open the Task.
    * Zero or more info events (`Strings`/`Texts`/`Context`/`Metrics`) that add properties values to the task. 
    * Closing event that closes the Task either successfully or with an error (`Success`/`Error`), note that as long as an event is in the works its status is `Unterminated`
  * Has a start-time, end-time and duration. 

##### Context-Aware: 
System events doesn't occur in a void.
 
Events always have context.  When did this event happen? Why did it happen? What was the path of events leading to this event? What were the values we encountered along the path?
It's rarely enough just to know that something happened and simple logging just doesn't let us know a lot more than that.

Timbermill does.

Every task in Timbermill points to a parent task and automatically keeps important information from it:
* Complete path of tasks leading to this task.
* Important properties from its ancestors.
* More!


### Getting Started

#### Requirements
* Elasticsearch cluster
* Java project (1.8+)
* Add dependency to maven

    
        <dependencies>
            <dependency>
                 <groupId>com.datorama</groupId>
                 <artifactId>timbermill-client</artifactId>
                 <version>2.1.0</version>
            </dependency>
            ...
        </dependencies>
             
        <build>
            <plugins>
                <plugin>
                    <groupId>org.codehaus.mojo</groupId>
                    <artifactId>aspectj-maven-plugin</artifactId>
                    <version>1.11</version>
                    <configuration>
                        <showWeaveInfo>true</showWeaveInfo>
                        <complianceLevel>1.8</complianceLevel>
                        <aspectLibraries>
                            <aspectLibrary>
                                <groupId>com.datorama.oss</groupId>
                                <artifactId>timbermill-client</artifactId>
                            </aspectLibrary>
                        </aspectLibraries>
                    </configuration>
                    <executions>
                        <execution>
                            <phase>process-sources</phase>
                            <goals>
                                <goal>compile</goal>
                                <goal>test-compile</goal>
                            </goals>
                        </execution>
                    </executions>
                </plugin>
                ...
            </plugins>
        </build>


#### How to use Timbermill

Timbermill is designed to be plug-and-play out-of-the-box.  
 
```
    public void main() {
        TimberLogger.bootstrap();
        
        log();
        
    }

    @TimberLog(name = "hello_world")
    public void log() {
        LogParams params = LogParams.create().string("foo", "bar").text("text", "This is a text!").metric("number", 42);
        TimberLogger.logParams(params);
        /**
         *  Your Code
         */
    }
```
                 
 This code bootstraps Timbermill with a local default Elasticsearch cluster (http://localhost:9200). It will write one task of name `hello_world` with the above properties to elasticsearch.
 
 ![Alt text](hello.png?raw=true "Kibana")
 

* A custom Elasticsearch URL (along with other [configurations](timbermill.configurations)) can be defined using `LocalOutputPipeConfig`.

```
LocalOutputPipeConfig.Builder builder = new LocalOutputPipeConfig.Builder().url("https://elasticsearch:9200");
LocalOutputPipeConfig config = builder.build();
TimberLog.bootstrap(config);
```

#### What can I do next?
 
 Get familiar with our [wiki](timbermill.wiki) so you get a better sense on how to properlly use Timbermill.
 
