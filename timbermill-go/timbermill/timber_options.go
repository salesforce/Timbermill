package timbermill

import (
	"time"
)

type TimberOption struct {
	dateToDelete time.Time
}

func NewTimberOptionWithDaysToKeep(days int) *TimberOption {
	if days > 0 && days < 365 {
		future := time.Now().AddDate(0, 0, days)
		return &TimberOption{dateToDelete: future}
	} else {
		return nil
	}
}

func NewTimberOptionWithDateToDelete(date time.Time) *TimberOption {
	now := time.Now()
	if date.Before(now) {
		date = now
	}

	return &TimberOption{
		dateToDelete: date,
	}
}
