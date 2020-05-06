// Copyright 2020 The Prometheus Authors
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

// This file holds boilerplate adapters for generic MergeSeriesSet and MergeQuerier functions, so we can have one optimized
// solution that works for both ChunkSeriesSet as well as SeriesSet.

package storage

import (
	"context"

	"github.com/prometheus/prometheus/pkg/labels"
)

type genericQuerier interface {
	baseQuerier
	Select(bool, *SelectHints, ...*labels.Matcher) (genericSeriesSet, error)
	Exec() (Warnings, error)
}

type genericSeriesSet interface {
	Next() bool
	At() Labels
	Err() error
}

type genericSeriesMergeFunc func(...Labels) Labels

type genericSeriesSetAdapter struct {
	SeriesSet
}

func (a *genericSeriesSetAdapter) At() Labels {
	return a.SeriesSet.At()
}

type genericChunkSeriesSetAdapter struct {
	ChunkSeriesSet
}

func (a *genericChunkSeriesSetAdapter) At() Labels {
	return a.ChunkSeriesSet.At()
}

type genericQuerierAdapter struct {
	baseQuerier

	// One-of. If both are set, Querier will be used.
	q  Querier
	cq ChunkQuerier

	w   Warnings
	err error
}

func (q *genericQuerierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (genericSeriesSet, error) {
	if q.q != nil {
		s, err := q.q.Select(sortSeries, hints, matchers...)
		if err != nil {
			q.err = err
			return &genericSeriesSetAdapter{s}, err
		}
		return &genericSeriesSetAdapter{s}, nil
	}
	s, err := q.cq.Select(sortSeries, hints, matchers...)
	if err != nil {
		q.err = err
		return &genericChunkSeriesSetAdapter{s}, err
	}
	return &genericChunkSeriesSetAdapter{s}, nil
}

func (q *genericQuerierAdapter) Exec() (Warnings, error) {
	return q.w, q.err
}

func newGenericQuerierFrom(q Querier) genericQuerier {
	return &genericQuerierAdapter{baseQuerier: q, q: q}
}

func newGenericQuerierFromChunk(cq ChunkQuerier) genericQuerier {
	return &genericQuerierAdapter{baseQuerier: cq, cq: cq}
}

type querierAdapter struct {
	genericQuerier

	w   Warnings
	err error
}

type seriesSetAdapter struct {
	genericSeriesSet
}

func (a *seriesSetAdapter) At() Series {
	return a.genericSeriesSet.At().(Series)
}

func (q *querierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (SeriesSet, error) {
	s, err := q.genericQuerier.Select(sortSeries, hints, matchers...)
	if err != nil {
		q.err = err
		return &seriesSetAdapter{s}, err
	}
	return &seriesSetAdapter{s}, nil
}

func (q *querierAdapter) Exec() (Warnings, error) {
	return q.w, q.err
}

type chunkQuerierAdapter struct {
	genericQuerier

	w   Warnings
	err error
}

type chunkSeriesSetAdapter struct {
	genericSeriesSet
}

func (a *chunkSeriesSetAdapter) At() ChunkSeries {
	return a.genericSeriesSet.At().(ChunkSeries)
}

func (q *chunkQuerierAdapter) Select(sortSeries bool, hints *SelectHints, matchers ...*labels.Matcher) (ChunkSeriesSet, error) {
	s, err := q.genericQuerier.Select(sortSeries, hints, matchers...)
	if err != nil {
		q.err = err
		return &chunkSeriesSetAdapter{s}, err
	}
	return &chunkSeriesSetAdapter{s}, nil
}

func (q *chunkQuerierAdapter) Exec() (Warnings, error) {
	return q.w, q.err
}

type seriesMergerAdapter struct {
	VerticalSeriesMergeFunc
	buf []Series
}

func (a *seriesMergerAdapter) Merge(s ...Labels) Labels {
	a.buf = a.buf[:0]
	for _, ser := range s {
		a.buf = append(a.buf, ser.(Series))
	}
	return a.VerticalSeriesMergeFunc(a.buf...)
}

type chunkSeriesMergerAdapter struct {
	VerticalChunkSeriesMergerFunc
	buf []ChunkSeries
}

func (a *chunkSeriesMergerAdapter) Merge(s ...Labels) Labels {
	a.buf = a.buf[:0]
	for _, ser := range s {
		a.buf = append(a.buf, ser.(ChunkSeries))
	}
	return a.VerticalChunkSeriesMergerFunc(a.buf...)
}

type genericLazySeriesSet struct {
	ctx     context.Context
	result  genericSeriesSet
	promise chan genericSeriesSet
}

func (s *genericLazySeriesSet) Next() bool {
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

func (s *genericLazySeriesSet) At() Labels {
	return s.result.At()
}

func (s *genericLazySeriesSet) Err() error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	return s.result.Err()
}
