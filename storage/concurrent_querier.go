// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/pkg/gate"
	"github.com/prometheus/prometheus/pkg/labels"
)

// NewConcurrentQuerier creates a new concurrent select manager.
func NewConcurrentQuerier(q Querier, ctx context.Context, concurrencyLimit int) ConcurrentQuerier {
	if concurrencyLimit > 1 {
		return &concurrentQuerier{Querier: q, ctx: ctx, concurrencyLimit: concurrencyLimit}
	}

	return &noopConcurrentQuerier{Querier: q}
}

type noopConcurrentQuerier struct {
	Querier

	warnings Warnings
	err      error
}

func (q *noopConcurrentQuerier) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (SeriesSet, Warnings, error) {
	set, wrn, err := q.Querier.Select(sortSeries, hints, matchers...)
	q.warnings = append(q.warnings, wrn...)
	if err != nil && q.err == nil {
		q.err = err
	}
	return set, q.warnings, err
}

func (q *noopConcurrentQuerier) Exec() (Warnings, error) {
	warnings, err := q.warnings, q.err
	q.warnings, q.err = nil, nil
	return warnings, err
}

// A concurrentQuerier processes added select requests concurrently.
// It limits amount of requests executed at the same time by the given concurrency limit.
type concurrentQuerier struct {
	Querier

	ctx              context.Context
	concurrencyLimit int

	selectGate *gate.Gate
	requests   []func() (Warnings, error)
}

func (q *concurrentQuerier) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (SeriesSet, Warnings, error) {
	promise := make(chan *selectResult, 1)
	q.requests = append(q.requests, func() (Warnings, error) {
		defer close(promise)
		set, w, err := q.Querier.Select(sortSeries, hints, matchers...)
		promise <- &selectResult{set: set, err: err}
		return w, err
	})
	return &lazySeriesSet{ctx: q.ctx, promise: promise}, nil, nil
}

func (q *concurrentQuerier) Exec() (Warnings, error) {
	defer func() { q.requests = nil }()

	reqSize := len(q.requests)
	if reqSize == 0 {
		return nil, nil
	}
	if reqSize == 1 {
		return q.requests[0]()
	}

	type response struct {
		warnings Warnings
		err      error
	}

	var (
		wg         sync.WaitGroup
		selectGate = gate.New(q.concurrencyLimit)
		responses  = make(chan response, reqSize)
	)

	wg.Add(reqSize)
	for _, req := range q.requests {
		go func(req func() (Warnings, error)) {
			defer wg.Done()

			if err := selectGate.Start(q.ctx); err != nil {
				responses <- response{nil, err}
				return
			}
			defer selectGate.Done()

			if wrn, err := req(); err != nil {
				responses <- response{wrn, err}
			}
		}(req)
	}
	wg.Wait()
	close(responses)

	var (
		warnings Warnings
		err      error
	)
	for res := range responses {
		if res.warnings != nil {
			warnings = append(warnings, res.warnings...)
		}
		if err != nil {
			err = res.err
		}
	}
	return warnings, err
}

type selectResult struct {
	set SeriesSet
	err error
}

type lazySeriesSet struct {
	ctx     context.Context
	promise chan *selectResult
	result  *selectResult
}

func (s *lazySeriesSet) Next() bool {
	if s.result == nil {
		select {
		case <-s.ctx.Done():
			return false
		case res, ok := <-s.promise:
			if !ok {
				return false
			}
			s.result = res
			return res.set.Next()
		}
	}

	return s.result.set.Next()
}

func (s *lazySeriesSet) At() Series {
	return s.result.set.At()
}

func (s *lazySeriesSet) Err() error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	if s.result.err != nil {
		return s.result.err
	}
	return s.result.set.Err()
}
