
# timbermill-client-go

this client is used to send logging data for timbermill server

## Example

```go
package main

import (
	"datorama.com/timbermill/timbermill"
)

func main() {
	timbermill.Init()
	
	params := timbermill.NewTimberParams()
	transaction := timbermill.TimberLoggerInstance.NewTransaction()
	transaction.Start("test-main", *params.Strings("test-strings", "123").Context("test-context", "context"))
	transaction.LogString("test-strings2", "123")

	transaction.Success(nil)
}
```

## Installation
```bash
go get github.com/datorama/Timbermill/tree/master/timbermill-go
```
It requires Go 1.11 or later due to usage of Go Modules.

## Configuration
It is possible to adjust different configurations for timbermill to work in a custom matter.
By default, timbermill uses a default configuration upon initialization 
```go
timbermill.Init()
```
it is possible to pass a custom `timber_configuration.Configuration`struct with different values.
for example:
```go
customConfig := timber_configuration.NewConfiguration(timber_configuration.WithBaseUrl("http://localhost:3000"))
```
all possible configurations and options can be found in `timber_configuration.Configuration` struct

## Basic Usage
* to start a new transaction simply call for a start method
```go
taskId := transaction.Start("test-main", timbermill.NewTimberParams())
```
when start is called after start that is not yet closed it crates a hierarchy
where a new task have a parent which is previous task.
* to log any data call one of the logging methods, for example:
```go
taskId := transaction.LogString("test-strings2", "123")
```
* to close the transaction call either for success and error function
```go
taskId := transaction.Error(errors.New("just an error"), timbermill.NewTimberParams().Text("success", "true"))
```
OR
```go
taskId := transaction.Success(nil)
```

* to log one time event use the `Spot` method
```go
transaction.Spot("this is spot event", *timbermill.NewTimberParams().Metrics("simple-metric", 1))
```

``for full timbermill api check `TimberTransaction` class``.

## Advanced Usage
it is possible to use timbermill in a cross-platform/service environment.
to use timbermill in a cross-platform simply pass upon transaction creation a previously open taskId
```go
transaction := timbermill.TimberLoggerInstance.OnGoingTransaction("[previously-opened-taskId]")
```
the rest will be taken care of by timbermill server
