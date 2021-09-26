package timbermill

import (
	"errors"
	"time"
)

type TimberOption struct {
	dateToDelete time.Time
}

func NewTimberOptionWithDaysToKeep(days int) (*TimberOption, error) {
	if days > 0 && days < 365 {
		future := time.Now().AddDate(0, 0, days)
		return &TimberOption{dateToDelete: future}, nil
	} else {
		return nil, errors.New("illegal number of days to keep use value between 1-365")
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
