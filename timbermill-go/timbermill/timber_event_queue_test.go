package timbermill

import (
	"datorama.com/timbermill/dto"
	"datorama.com/timbermill/timber_configuration"
	"datorama.com/timbermill/utils"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

var defaultConfiguration = timber_configuration.InMemoryQueueConfiguration{
	MaxBufferSize:                10000,
	MaxSecondsBeforeBatchTimeout: 1,
	MaxEventsBatchSize:           10000,
}

func TestInMemoryQueue_enqueueEvent(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)

	assert.Equal(t, queue.deque.Len(), 0)

	err := queue.EnqueueEvent(defaultEvent())
	assert.Empty(t, err)
	assert.Equal(t, queue.deque.Len(), 1)
}

func TestInMemoryQueue_enqueueEventMaxBufferSizeReached(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)
	queue.maxBufferSize = 500

	assert.Equal(t, queue.deque.Len(), 0)

	err := queue.EnqueueEvent(defaultEvent())
	assert.Empty(t, err)

	err = queue.EnqueueEvent(defaultEvent())
	expectedErrorMsg := "max buffer size was accessed"
	assert.EqualErrorf(t, err, expectedErrorMsg, "Error should be: %v, got: %v", expectedErrorMsg, err)
	assert.Equal(t, queue.deque.Len(), 1)
}

func TestInMemoryQueue_dequeueEvent(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)

	assert.Equal(t, queue.deque.Len(), 0)

	event := defaultEvent()
	eventSize, err := utils.GetRealSizeOf(event)
	assert.Empty(t, err)

	event.TaskId = "456"
	err = queue.EnqueueEvent(event)
	assert.Empty(t, err)

	err = queue.EnqueueEvent(defaultEvent())
	assert.Empty(t, err)
	assert.Equal(t, queue.currentSize, eventSize*2)

	dequedEvent, size := queue.dequeEvent()
	assert.Equal(t, dequedEvent.TaskId, "456")
	assert.Equal(t, size, eventSize)
	assert.Equal(t, queue.currentSize, eventSize)
	assert.Equal(t, queue.deque.Len(), 1)
}

func TestInMemoryQueue_processEvent(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)

	event := defaultEvent()
	event.Text = map[string]string{"text.dot.dot": strings.Repeat("a", maxCharsAllowedForAnalyzedFields+3)}
	event.Metrics = map[string]int{"metrics.dot": 5}
	event.Strings = map[string]string{"string.dot": strings.Repeat("s", maxCharsAllowedForNonAnalyzedFields+4)}

	processedEvent := queue.processEvent(event)
	assert.Equal(t, processedEvent.Metrics, map[string]int{"metrics_dot": 5})
	assert.Equal(t, processedEvent.Strings, map[string]string{"string_dot": strings.Repeat("s", maxCharsAllowedForNonAnalyzedFields)})
	assert.Equal(t, len(processedEvent.Text["text_dot_dot"]), maxCharsAllowedForAnalyzedFields)
}

func TestInMemoryQueue_events(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)

	assert.Equal(t, queue.deque.Len(), 0)
	event := defaultEvent()
	eventSize, err := utils.GetRealSizeOf(event)
	assert.Empty(t, err)

	queue.EnqueueEvent(event)
	queue.EnqueueEvent(defaultEvent())
	queue.EnqueueEvent(defaultEvent())
	assert.Equal(t, queue.currentSize, eventSize*3)

	events := queue.Events()
	assert.Equal(t, len(events), 3)
	assert.Equal(t, queue.currentSize, 0)
}

func TestInMemoryQueue_eventsMaxEventsBatchSize(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)

	assert.Equal(t, queue.deque.Len(), 0)

	event := defaultEvent()
	eventSize, err := utils.GetRealSizeOf(event)
	assert.Empty(t, err)

	queue.maxEventsBatchSize = eventSize * 2
	queue.EnqueueEvent(event)
	queue.EnqueueEvent(defaultEvent())
	queue.EnqueueEvent(defaultEvent())

	events := queue.Events()
	assert.Equal(t, len(events), 2)
	assert.Equal(t, queue.currentSize, eventSize)
	assert.Equal(t, queue.deque.Len(), 1)
}

func TestInMemoryQueue_eventsExceededMaxTimeToWait(t *testing.T) {
	queue := (*newInMemoryTimberEventQueue(defaultConfiguration)).(*inMemoryTimberEventQueue)

	assert.Equal(t, queue.deque.Len(), 0)

	queue.EnqueueEvent(defaultEvent())

	start := time.Now()
	queue.maxSecondsBeforeBatchTimeout = 3
	queue.Events()
	exucationTime := time.Since(start)
	assert.Greater(t, int64(exucationTime), int64(3*time.Second))
}

func defaultEvent() *dto.TimbermillEvent {
	return &dto.TimbermillEvent{
		TaskId:  "123",
		Text:    map[string]string{"1": "1"},
		Metrics: map[string]int{"1": 12},
	}
}
