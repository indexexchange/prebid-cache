package backends

import (
	"context"
	"errors"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	as_types "github.com/aerospike/aerospike-client-go/types"
	"github.com/prebid/prebid-cache/config"
	"github.com/prebid/prebid-cache/metrics"
	"github.com/prebid/prebid-cache/utils"
	log "github.com/sirupsen/logrus"
)

const binValue = "value"

// Wrapper for the Aerospike client
type AerospikeDB interface {
	NewUuidKey(namespace string, set string, key string) (*as.Key, error)
	Get(key *as.Key) (*as.Record, error)
	Put(policy *as.WritePolicy, key *as.Key, binMap as.BinMap) error
}

type AerospikeDBClient struct {
	client *as.Client
}

func (db AerospikeDBClient) Get(key *as.Key) (*as.Record, error) {
	return db.client.Get(nil, key, binValue)
}

func (db AerospikeDBClient) Put(policy *as.WritePolicy, key *as.Key, binMap as.BinMap) error {
	return db.client.Put(policy, key, binMap)
}

func (db *AerospikeDBClient) NewUuidKey(namespace string, setName string, key string) (*as.Key, error) {
	return as.NewKey(namespace, setName, key)
}

// Instantiates, and configures the Aerospike client, it also performs Get and Put operations and monitors results
type AerospikeBackend struct {
	namespace         string
	client            AerospikeDB
	metrics           *metrics.Metrics
	defaultSet        string
	customSets        map[string]string
	defaultPutOptions PutOptions
}

func NewAerospikeBackend(cfg config.Aerospike, metrics *metrics.Metrics) *AerospikeBackend {
	var hosts []*as.Host

	clientPolicy := as.NewClientPolicy()
	// cfg.User and cfg.Password are optional parameters
	// if left blank in the config, they will default to the empty
	// string and be ignored
	clientPolicy.User = cfg.User
	clientPolicy.Password = cfg.Password

	if len(cfg.Host) > 1 {
		hosts = append(hosts, as.NewHost(cfg.Host, cfg.Port))
		log.Info("config.backend.aerospike.host is being deprecated in favor of config.backend.aerospike.hosts")
	}
	for _, host := range cfg.Hosts {
		hosts = append(hosts, as.NewHost(host, cfg.Port))
	}

	if len(cfg.DefaultSet) == 0 {
		log.Fatalf("config.backend.aerospike.default_set is not configured")
		panic("Aerospike backend failure no DefaultSet configured")
	}

	client, err := as.NewClientWithPolicyAndHost(clientPolicy, hosts...)
	if err != nil {
		log.Fatalf("%v", formatAerospikeError(err).Error())
		panic("AerospikeBackend failure. This shouldn't happen.")
	}
	log.Infof("Connected to Aerospike host(s) %v on port %d", append(cfg.Hosts, cfg.Host), cfg.Port)

	return &AerospikeBackend{
		namespace:  cfg.Namespace,
		client:     &AerospikeDBClient{client},
		metrics:    metrics,
		defaultSet: cfg.DefaultSet,
		customSets: cfg.CustomSets,
		defaultPutOptions: PutOptions{
			WriteTimeoutMs: cfg.WriteTimeoutMs,
			WriteRetries:   cfg.WriteRetries,
		},
	}

}

func (a *AerospikeBackend) FetchSourceSet(source string) string {
	setName := a.customSets[source]

	if setName == "" {
		setName = a.defaultSet
	}

	return setName
}

func (a *AerospikeBackend) Get(ctx context.Context, key string, source string) (string, error) {

	setName := a.FetchSourceSet(source)

	asKey, err := a.client.NewUuidKey(a.namespace, setName, key)
	if err != nil {
		return "", formatAerospikeError(err)
	}
	rec, err := a.client.Get(asKey)
	if err != nil {
		return "", formatAerospikeError(err)
	}
	if rec == nil {
		return "", formatAerospikeError(errors.New("Nil record"))
	}

	value, found := rec.Bins[binValue]
	if !found {
		return "", formatAerospikeError(errors.New("No 'value' bucket found"))
	}

	str, isString := value.(string)
	if !isString {
		return "", formatAerospikeError(errors.New("Unexpected non-string value found"))
	}

	return str, nil
}

func (a *AerospikeBackend) Put(ctx context.Context, key string, value string, ttlSeconds int, putOptions PutOptions) error {

	setName := a.FetchSourceSet(putOptions.Source)

	asKey, err := a.client.NewUuidKey(a.namespace, setName, key)
	if err != nil {
		return formatAerospikeError(err)
	}

	bins := as.BinMap{binValue: value}
	policy := &as.WritePolicy{
		BasePolicy: as.BasePolicy{
			TotalTimeout: time.Duration(a.defaultPutOptions.WriteTimeoutMs) * time.Millisecond,
			MaxRetries:   a.defaultPutOptions.WriteRetries,
		},
		Expiration: uint32(ttlSeconds),
	}

	if putOptions.WriteTimeoutMs != 0 {
		policy.TotalTimeout = time.Duration(putOptions.WriteTimeoutMs) * time.Millisecond
	}
	if putOptions.WriteRetries != 0 {
		policy.MaxRetries = putOptions.WriteRetries
	}

	if err := a.client.Put(policy, asKey, bins); err != nil {
		return formatAerospikeError(err)
	}

	return nil
}

func formatAerospikeError(err error) error {
	if err != nil {
		if aerr, ok := err.(as_types.AerospikeError); ok {
			if aerr.ResultCode() == as_types.KEY_NOT_FOUND_ERROR {
				return utils.KeyNotFoundError{}
			}
		}
		return errors.New(err.Error())
	}
	return err
}
