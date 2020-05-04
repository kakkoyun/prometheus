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
)

// A sequentialSelectManager processes added select requests immediately in given order.
type sequentialSelectManager struct {
	warnings Warnings
	err      error
}

// NewSequentialSelectManager creates a new sequential query manager.
func NewSequentialSelectManager() *sequentialSelectManager {
	return &sequentialSelectManager{}
}

func (qm *sequentialSelectManager) Add(req func() (Warnings, error)) error {
	wrn, err := req()
	qm.warnings = append(qm.warnings, wrn...)
	return err
}

func (qm *sequentialSelectManager) Run(ctx context.Context) (Warnings, error) {
	warnings, err := qm.warnings, qm.err
	qm.warnings, qm.err = nil, nil
	return warnings, err
}

// A concurrentSelectManager processes added select requests concurrently.
// It limits amount of requests executed at the same time by given concurrency limit.
type concurrentSelectManager struct {
	selectGate *gate.Gate
	requests   []func() (Warnings, error)
}

// NewConcurrentSelectManager creates a new concurrent query manager.
func NewConcurrentSelectManager(concurrencyLimit int) *concurrentSelectManager {
	return &concurrentSelectManager{selectGate: gate.New(concurrencyLimit)}
}

func (qm *concurrentSelectManager) Add(req func() (Warnings, error)) error {
	qm.requests = append(qm.requests, req)
	return nil
}

func (qm *concurrentSelectManager) Run(ctx context.Context) (Warnings, error) {
	defer func() { qm.requests = nil }()

	reqSize := len(qm.requests)
	if reqSize == 1 {
		return qm.requests[0]()
	}

	type result struct {
		warnings Warnings
		err      error
	}

	var (
		wg      sync.WaitGroup
		results = make(chan result, reqSize)
	)

	wg.Add(reqSize)
	for _, req := range qm.requests {
		go func(req func() (Warnings, error)) {
			defer wg.Done()

			if err := qm.selectGate.Start(ctx); err != nil {
				results <- result{nil, err}
				return
			}
			defer qm.selectGate.Done()

			if wrn, err := req(); err != nil {
				results <- result{wrn, err}
			}
		}(req)
	}
	wg.Wait()
	close(results)

	select {
	case <-ctx.Done():
		// TODO(kakkoyun): Not sure, it's necessary.
		return nil, ctx.Err()
	case res := <-results: // Only return the first error.
		return res.warnings, res.err
	}
}
