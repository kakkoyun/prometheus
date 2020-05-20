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

package remote

import (
	"context"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/storage"
)

var errPrematurelyClosedPromise = errors.New("promise channel closed before result received")

type asyncSeriesSet struct {
	ctx     context.Context
	promise chan storage.SeriesSet
	result  storage.SeriesSet
}

func (s *asyncSeriesSet) Next() bool {
	if s.result == nil {
		select {
		case <-s.ctx.Done():
			return false
		case res, ok := <-s.promise:
			if !ok {
				return false
			}
			s.result = res
			return res.Next()
		}
	}

	return s.result.Next()
}

func (s *asyncSeriesSet) At() storage.Series {
	return s.result.At()
}

func (s *asyncSeriesSet) Err() error {
	if err := s.ctx.Err(); err != nil {
		return err
	}

	if s.result == nil {
		return errPrematurelyClosedPromise
	}

	return s.result.Err()
}

func (s *asyncSeriesSet) Warnings() storage.Warnings {
	if s.result != nil {
		return s.result.Warnings()
	}
	return nil
}
