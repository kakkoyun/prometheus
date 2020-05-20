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
	"errors"
	"testing"

	"github.com/prometheus/prometheus/storage"
)

func TestAsyncSeriesSet_Next(t *testing.T) {
	type fields struct {
		ctx     context.Context
		promise chan storage.SeriesSet
	}

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()

	closedChannel := make(chan storage.SeriesSet)
	close(closedChannel)

	channel := make(chan storage.SeriesSet, 1)
	channel <- storage.EmptySeriesSet()
	close(channel)

	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "returns false when context cancelled",
			fields: fields{
				ctx: cancelledContext,
			},
			want: false,
		},
		{
			name: "returns false when channel closed",
			fields: fields{
				ctx:     context.TODO(),
				promise: closedChannel,
			},
			want: false,
		},
		{
			name: "proxies call to underlying set",
			fields: fields{
				ctx:     context.TODO(),
				promise: channel,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &asyncSeriesSet{
				ctx:     tt.fields.ctx,
				promise: tt.fields.promise,
			}
			if got := s.Next(); got != tt.want {
				t.Errorf("asyncSeriesSet.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAsyncSeriesSet_Err(t *testing.T) {
	type fields struct {
		ctx     context.Context
		promise chan storage.SeriesSet
	}

	cancelledContext, cancel := context.WithCancel(context.Background())
	cancel()

	closedChannel := make(chan storage.SeriesSet)
	close(closedChannel)

	errRemote := errors.New("remote error")
	channel := make(chan storage.SeriesSet, 1)
	channel <- storage.ErrSeriesSet(errRemote)
	close(channel)

	tests := []struct {
		name   string
		fields fields
		want   error
	}{
		{
			name: "returns error from context when context cancelled",
			fields: fields{
				ctx: cancelledContext,
			},
			want: context.Canceled,
		},
		{
			name: "returns sentinel error when channel closed",
			fields: fields{
				ctx:     context.TODO(),
				promise: closedChannel,
			},
			want: errPrematurelyClosedPromise,
		},
		{
			name: "proxies call to underlying set",
			fields: fields{
				ctx:     context.TODO(),
				promise: channel,
			},
			want: errRemote,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &asyncSeriesSet{
				ctx:     tt.fields.ctx,
				promise: tt.fields.promise,
			}
			for s.Next() {
			}
			if err := s.Err(); !errors.Is(err, tt.want) {
				t.Errorf("asyncSeriesSet.Err() error = %v, want %v", err, tt.want)
			}
		})
	}
}
