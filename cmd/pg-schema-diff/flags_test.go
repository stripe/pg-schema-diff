package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogFmtToMap(t *testing.T) {
	type args struct {
		logFmt string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{
			name: "empty string",
			args: args{
				logFmt: "",
			},
			want:    map[string]string{},
			wantErr: false,
		},
		{
			name: "single key value pair",
			args: args{
				logFmt: "key=value",
			},
			want:    map[string]string{"key": "value"},
			wantErr: false,
		},
		{
			name: "multiple key value pairs",
			args: args{
				logFmt: "key1=value1 key2=value2",
			},
			want:    map[string]string{"key1": "value1", "key2": "value2"},
			wantErr: false,
		},
		{
			name: "duplicate key",
			args: args{
				logFmt: "key=value1 key=value2",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "multiple records",
			args: args{
				logFmt: "key1=value1 key2=value2\nkey3=value3",
			},
			want:    map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := LogFmtToMap(tt.args.logFmt)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
