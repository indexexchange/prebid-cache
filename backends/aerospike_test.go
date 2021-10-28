package backends

import (
	"context"
	"fmt"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	as_types "github.com/aerospike/aerospike-client-go/types"
	"github.com/prebid/prebid-cache/config"
	"github.com/prebid/prebid-cache/metrics/metricstest"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"

	"github.com/stretchr/testify/assert"
)

// Mock Aerospike client that always throws an error
type errorProneAerospikeClient struct {
	errorThrowingFunction string
}

func NewErrorProneAerospikeClient(funcName string) *errorProneAerospikeClient {
	return &errorProneAerospikeClient{
		errorThrowingFunction: funcName,
	}
}

func (c *errorProneAerospikeClient) NewUuidKey(namespace string, set string, key string) (*as.Key, error) {
	if c.errorThrowingFunction == "TEST_KEY_GEN_ERROR" {
		return nil, as_types.NewAerospikeError(as_types.NOT_AUTHENTICATED)
	}
	return nil, nil
}

func (c *errorProneAerospikeClient) Get(key *as.Key) (*as.Record, error) {
	if c.errorThrowingFunction == "TEST_GET_ERROR" {
		return nil, as_types.NewAerospikeError(as_types.KEY_NOT_FOUND_ERROR)
	} else if c.errorThrowingFunction == "TEST_NO_BUCKET_ERROR" {
		return &as.Record{Bins: as.BinMap{"AnyKey": "any_value"}}, nil
	} else if c.errorThrowingFunction == "TEST_NON_STRING_VALUE_ERROR" {
		return &as.Record{Bins: as.BinMap{binValue: 0.0}}, nil
	}
	return nil, nil
}

func (c *errorProneAerospikeClient) Put(policy *as.WritePolicy, key *as.Key, binMap as.BinMap) error {
	if c.errorThrowingFunction == "TEST_PUT_ERROR" {
		return as_types.NewAerospikeError(as_types.KEY_EXISTS_ERROR)
	}
	return nil
}

// Mock Aerospike client that does not throw errors
type goodAerospikeClient struct {
	records                map[string]*as.Record
	expectedWriteTimeoutMs int
	expectedRetries        int
}

func NewGoodAerospikeClient(expectedWriteTimeoutMs int, expectedRetries int) *goodAerospikeClient {
	return &goodAerospikeClient{
		records: map[string]*as.Record{
			"defaultKey": {
				Bins: as.BinMap{binValue: "Default value"},
			},
		},
		expectedWriteTimeoutMs: expectedWriteTimeoutMs,
		expectedRetries:        expectedRetries,
	}
}

func (c *goodAerospikeClient) Get(aeKey *as.Key) (*as.Record, error) {
	if aeKey != nil && aeKey.Value() != nil {

		key := aeKey.Value().String()

		if rec, found := c.records[key]; found {
			return rec, nil
		}
	}
	return nil, as_types.NewAerospikeError(as_types.KEY_NOT_FOUND_ERROR)
}

func (c *goodAerospikeClient) Put(policy *as.WritePolicy, aeKey *as.Key, binMap as.BinMap) error {
	if policy.TotalTimeout != time.Duration(c.expectedWriteTimeoutMs)*time.Millisecond || policy.MaxRetries != c.expectedRetries {
		return as_types.NewAerospikeError(as_types.ILLEGAL_STATE)
	}
	if aeKey != nil && aeKey.Value() != nil {
		key := aeKey.Value().String()
		c.records[key] = &as.Record{
			Bins: binMap,
		}
		return nil
	}
	return as_types.NewAerospikeError(as_types.KEY_MISMATCH)
}

func (c *goodAerospikeClient) NewUuidKey(namespace string, setName string, key string) (*as.Key, error) {
	return as.NewKey(namespace, setName, key)
}

func TestNewAerospikeBackend(t *testing.T) {
	type logEntry struct {
		msg string
		lvl logrus.Level
	}

	testCases := []struct {
		desc               string
		inCfg              config.Aerospike
		expectPanic        bool
		expectedLogEntries []logEntry
	}{
		{
			desc: "Unable to connect hosts fakeTestUrl panic and log fatal error when passed additional hosts",
			inCfg: config.Aerospike{
				Hosts:      []string{"foo.com", "bat.com"},
				Port:       8888,
				DefaultSet: "uuid",
			},
			expectPanic: true,
			expectedLogEntries: []logEntry{

				{
					msg: "Failed to connect to host(s): [foo.com:8888 bat.com:8888]; error: Connecting to the cluster timed out.",
					lvl: logrus.FatalLevel,
				},
			},
		},
		{
			desc: "Unable to connect host and hosts panic and log fatal error when passed additional hosts",
			inCfg: config.Aerospike{
				Host:       "fakeTestUrl.foo",
				Hosts:      []string{"foo.com", "bat.com"},
				Port:       8888,
				DefaultSet: "uuid",
			},
			expectPanic: true,
			expectedLogEntries: []logEntry{
				{
					msg: "config.backend.aerospike.host is being deprecated in favor of config.backend.aerospike.hosts",
					lvl: logrus.InfoLevel,
				},
				{
					msg: "Failed to connect to host(s): [fakeTestUrl.foo:8888 foo.com:8888 bat.com:8888]; error: Connecting to the cluster timed out.",
					lvl: logrus.FatalLevel,
				},
			},
		},
		{
			desc: "Unable to connect host panic and log fatal error",
			inCfg: config.Aerospike{
				Host:       "fakeTestUrl.foo",
				Port:       8888,
				DefaultSet: "uuid",
			},
			expectPanic: true,
			expectedLogEntries: []logEntry{
				{
					msg: "config.backend.aerospike.host is being deprecated in favor of config.backend.aerospike.hosts",
					lvl: logrus.InfoLevel,
				},
				{
					msg: "Failed to connect to host(s): [fakeTestUrl.foo:8888]; error: Connecting to the cluster timed out.",
					lvl: logrus.FatalLevel,
				},
			},
		},
		{
			desc: "No DefaultSet is configured and log fatal error ",
			inCfg: config.Aerospike{
				Host:  "fakeTestUrl.foo",
				Hosts: []string{"foo.com", "bat.com"},
				Port:  8888,
			},
			expectPanic: true,
			expectedLogEntries: []logEntry{
				{
					msg: "config.backend.aerospike.host is being deprecated in favor of config.backend.aerospike.hosts",
					lvl: logrus.InfoLevel,
				},
				{
					msg: "config.backend.aerospike.default_set is not configured",
					lvl: logrus.FatalLevel,
				},
			},
		},
	}

	// logrus entries will be recorded to this `hook` object so we can compare and assert them
	hook := test.NewGlobal()

	//substitute logger exit function so execution doesn't get interrupted when log.Fatalf() call comes
	defer func() { logrus.StandardLogger().ExitFunc = nil }()
	logrus.StandardLogger().ExitFunc = func(int) {}

	for _, test := range testCases {
		// Run test
		assert.Panics(t, func() { NewAerospikeBackend(test.inCfg, nil) }, "Aerospike library's NewClientWithPolicyAndHost() should have thrown an error and didn't, hence the panic didn't happen")
		if assert.Len(t, hook.Entries, len(test.expectedLogEntries), test.desc) {
			for i := 0; i < len(test.expectedLogEntries); i++ {
				assert.Equal(t, test.expectedLogEntries[i].msg, hook.Entries[i].Message, test.desc)
				assert.Equal(t, test.expectedLogEntries[i].lvl, hook.Entries[i].Level, test.desc)
			}
		}

		//Reset log after every test and assert successful reset
		hook.Reset()
		assert.Nil(t, hook.LastEntry())
	}
}

func TestFetchSourceSet(t *testing.T) {
	testCases := []struct {
		desc            string
		customSets      map[string]string
		expectedSetName string
	}{
		{
			desc:            "No Custom sets Defined",
			customSets:      nil,
			expectedSetName: "uuid",
		},
		{
			desc:            "Custom sets defined but source doesnt match",
			customSets:      map[string]string{"no_match_source": "foo"},
			expectedSetName: "uuid",
		},
		{
			desc:            "Custom sets defined and source matches",
			customSets:      map[string]string{"match_source": "bar"},
			expectedSetName: "bar",
		},
	}

	for _, test := range testCases {
		aerospikeBackend := &AerospikeBackend{
			metrics:    metricstest.CreateMockMetrics(),
			defaultSet: "uuid",
			customSets: test.customSets,
		}
		setName := aerospikeBackend.FetchSourceSet("match_source")
		assert.Equal(t, test.expectedSetName, setName, test.desc)

	}

}

func TestFormatAerospikeError(t *testing.T) {
	testCases := []struct {
		desc        string
		inErr       error
		expectedErr error
	}{
		{
			desc:        "Nil error",
			inErr:       nil,
			expectedErr: nil,
		},
		{
			desc:        "Non-nil error, print without a caller",
			inErr:       fmt.Errorf("client.Get returned nil record"),
			expectedErr: fmt.Errorf("client.Get returned nil record"),
		},
		{
			desc:        "Non-nil error, comes with a caller",
			inErr:       fmt.Errorf("client.Get returned nil record"),
			expectedErr: fmt.Errorf("client.Get returned nil record"),
		},
		{
			desc:        "Non-nil error, comes with more than one callers",
			inErr:       fmt.Errorf("client.Get returned nil record"),
			expectedErr: fmt.Errorf("client.Get returned nil record"),
		},
		{
			desc:        "Aerospike error, comes with a caller",
			inErr:       as_types.NewAerospikeError(as_types.SERVER_NOT_AVAILABLE),
			expectedErr: fmt.Errorf("Server is not accepting requests."),
		},
		{
			desc:        "Aerospike KEY_NOT_FOUND_ERROR error, attach our GetKeyNotFound constant",
			inErr:       as_types.NewAerospikeError(as_types.KEY_NOT_FOUND_ERROR),
			expectedErr: fmt.Errorf("Key not found"),
		},
	}
	for _, test := range testCases {
		actualErr := formatAerospikeError(test.inErr)
		if test.expectedErr == nil {
			assert.Nil(t, actualErr, test.desc)
		} else {
			assert.Equal(t, test.expectedErr.Error(), actualErr.Error(), test.desc)
		}
	}
}

func TestClientGet(t *testing.T) {
	aerospikeBackend := &AerospikeBackend{
		metrics: metricstest.CreateMockMetrics(),
	}

	testCases := []struct {
		desc              string
		inAerospikeClient AerospikeDB
		expectedValue     string
		expectedErrorMsg  string
	}{
		{
			desc:              "AerospikeBackend.Get() throws error when trying to generate new key",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_KEY_GEN_ERROR"),
			expectedValue:     "",
			expectedErrorMsg:  "Not authenticated",
		},
		{
			desc:              "AerospikeBackend.Get() throws error when 'client.Get(..)' gets called",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_GET_ERROR"),
			expectedValue:     "",
			expectedErrorMsg:  "Key not found",
		},
		{
			desc:              "AerospikeBackend.Get() throws error when 'client.Get(..)' returns a nil record",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_NIL_RECORD_ERROR"),
			expectedValue:     "",
			expectedErrorMsg:  "Nil record",
		},
		{
			desc:              "AerospikeBackend.Get() throws error no BIN_VALUE bucket is found",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_NO_BUCKET_ERROR"),
			expectedValue:     "",
			expectedErrorMsg:  "No 'value' bucket found",
		},
		{
			desc:              "AerospikeBackend.Get() returns a record that does not store a string",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_NON_STRING_VALUE_ERROR"),
			expectedValue:     "",
			expectedErrorMsg:  "Unexpected non-string value found",
		},
		{
			desc:              "AerospikeBackend.Get() does not throw error",
			inAerospikeClient: NewGoodAerospikeClient(0, 0),
			expectedValue:     "Default value",
			expectedErrorMsg:  "",
		},
	}

	for _, tt := range testCases {
		// Assign aerospike backend cient
		aerospikeBackend.client = tt.inAerospikeClient

		// Run test
		actualValue, actualErr := aerospikeBackend.Get(context.TODO(), "defaultKey", "defaultSet")

		// Assertions
		assert.Equal(t, tt.expectedValue, actualValue, tt.desc)

		if tt.expectedErrorMsg == "" {
			assert.Nil(t, actualErr, tt.desc)
		} else {
			assert.Equal(t, tt.expectedErrorMsg, actualErr.Error(), tt.desc)
		}
	}
}

func TestClientPut(t *testing.T) {
	aerospikeBackend := &AerospikeBackend{
		metrics: metricstest.CreateMockMetrics(),
	}

	testCases := []struct {
		desc                    string
		inAerospikeClient       AerospikeDB
		inKey                   string
		inValueToStore          string
		inDefaultWriteTimeoutMs int
		inDefaultWriteRetries   int
		inPutOptions            PutOptions
		expectedStoredVal       string
		expectedErrorMsg        string
	}{
		{
			desc:              "AerospikeBackend.Put() throws error when trying to generate new key",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_KEY_GEN_ERROR"),
			inKey:             "testKey",
			inValueToStore:    "not default value",
			expectedStoredVal: "",
			expectedErrorMsg:  "Not authenticated",
		},
		{
			desc:              "AerospikeBackend.Put() throws error when 'client.Put(..)' gets called",
			inAerospikeClient: NewErrorProneAerospikeClient("TEST_PUT_ERROR"),
			inKey:             "testKey",
			inValueToStore:    "not default value",
			expectedStoredVal: "",
			expectedErrorMsg:  "Key already exists",
		},
		{
			desc:                    "AerospikeBackend.Put() does not throw error and uses backend defaults",
			inAerospikeClient:       NewGoodAerospikeClient(10, 2),
			inKey:                   "testKey",
			inValueToStore:          "any value",
			inDefaultWriteTimeoutMs: 10,
			inDefaultWriteRetries:   2,
			inPutOptions:            PutOptions{Source: "defaultSet"},
			expectedStoredVal:       "any value",
			expectedErrorMsg:        "",
		},
		{
			desc:                    "AerospikeBackend.Put() does not throw error and uses overridden options",
			inAerospikeClient:       NewGoodAerospikeClient(1, 1),
			inKey:                   "testKey",
			inValueToStore:          "any value",
			inDefaultWriteTimeoutMs: 10,
			inDefaultWriteRetries:   2,
			inPutOptions:            PutOptions{WriteTimeoutMs: 1, WriteRetries: 1, Source: "defaultSet"},
			expectedStoredVal:       "any value",
			expectedErrorMsg:        "",
		},
	}

	for _, tt := range testCases {
		// Assign aerospike backend cient
		aerospikeBackend.client = tt.inAerospikeClient
		aerospikeBackend.defaultPutOptions.WriteTimeoutMs = tt.inDefaultWriteTimeoutMs
		aerospikeBackend.defaultPutOptions.WriteRetries = tt.inDefaultWriteRetries

		// Run test
		actualErr := aerospikeBackend.Put(context.TODO(), tt.inKey, tt.inValueToStore, 0, tt.inPutOptions)

		// Assert Put error
		if tt.expectedErrorMsg != "" {
			assert.Equal(t, tt.expectedErrorMsg, actualErr.Error(), tt.desc)
		} else {
			assert.Nil(t, actualErr, tt.desc)

			// Assert Put() sucessfully logged "not default value" under "testKey":
			storedValue, getErr := aerospikeBackend.Get(context.TODO(), tt.inKey, "defaultSet")

			assert.Nil(t, getErr, tt.desc)
			assert.Equal(t, tt.inValueToStore, storedValue, tt.desc)
		}
	}
}
