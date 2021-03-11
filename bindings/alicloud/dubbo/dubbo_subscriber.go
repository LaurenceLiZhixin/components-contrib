package dubbo

import (
	"sync"

	dubboConstant "github.com/apache/dubbo-go/common/constant"

	"github.com/dapr/dapr/pkg/logger"

	"strings"

	"github.com/dapr/components-contrib/bindings"
)

const (
	product = "dapr"
	action  = "dubbo-subscribe"
)

var dubboSubscriber *DUBBOSubscriber

// DUBBOSubscriber is an impl of oubput binding, it can get init consumer side invoker to rpcServices, and do
type DUBBOSubscriber struct {
	timeout       int64
	interfaceName string
	version       string
	group         string
	rpcServices   map[string]*ConsumerConfigAPIBean
	registerLock  sync.Mutex
	logger        logger.Logger
	metadata      bindings.Metadata
}

// NewDUBBOSubscriber return new dubbo subscriber with @logger
func NewDUBBOSubscriber(logger logger.Logger) *DUBBOSubscriber {
	dubboSubscriber = &DUBBOSubscriber{
		registerLock: sync.Mutex{},
		rpcServices:  make(map[string]*ConsumerConfigAPIBean, 4),
		logger:       logger,
	}
	return dubboSubscriber
}

// Init init dubbo subscriber from dapr @metadata
func (ds *DUBBOSubscriber) Init(metadata bindings.Metadata) error {
	for _, item := range FailBackState(product, action) {
		if len(item) == 0 {
			continue
		}
		interfaceName, version, group := recoverKey(item)
		ds.appendGenericAndProxyService(interfaceName, version, group, false)
	}
	ds.metadata = metadata
	return nil
}

// getProxyService get rpc service from cache by @interfaceName, @version and @group
// if not exist, it will try to refere
func (ds *DUBBOSubscriber) getProxyService(interfaceName, version, group string) *ProxyService {
	key := key(interfaceName, version, group)
	rpcService := ds.rpcServices[key]
	if rpcService == nil {
		ds.appendGenericAndProxyService(interfaceName, version, group, true)
		rpcService = ds.rpcServices[key]
	}
	return rpcService.GetProxyService()
}

// appendGenericAndProxyService try to new dubbo consumer reference from given @interfaceName, @version and @group
func (ds *DUBBOSubscriber) appendGenericAndProxyService(interfaceName, version, group string, stateUpdate bool) {
	if len(interfaceName) == 0 {
		ds.logger.Info("appaendGenericService with empty interfaceName")
		return
	}
	k := key(interfaceName, version, group)
	if ds.rpcServices[k] != nil {
		return
	}
	ds.registerLock.Lock()
	defer ds.registerLock.Unlock()
	if ds.rpcServices[k] != nil {
		return
	}

	params := make(map[string]string)
	consumerConfigAPI := NewConsumerConfigAPIBean(ds.logger).
		WithInterfaceName(interfaceName).
		WithGroup(group).
		WithVersion(version).
		WithProtocol("dubbo").
		WithLB(dubboConstant.DEFAULT_LOADBALANCE).
		WithCluster("failover").
		WithRetries("0").
		WithCheck(false).
		WithParam(params).WithGeneric().WithProxy()

	consumerConfigAPI.Init()

	if stateUpdate && len(interfaceName) != 0 {
		StoreState(product, action, k)
	}

	ds.rpcServices[k] = consumerConfigAPI
}

// recoverKey get interfaceName, version and group from @item
func recoverKey(item string) (string, string, string) {
	if len(item) == 0 {
		return "", "", ""
	}
	sp1 := strings.Index(item, "/")
	sp2 := strings.Index(item, ":")
	group := item[0:sp1]
	interfaceName := item[sp1+1 : sp2]
	version := item[sp2+1 : len(item)]
	return interfaceName, version, group
}

// key get unique service key from @interfaceName @version and group
func key(interfaceName, version, group string) string {
	return group + "/" + interfaceName + ":" + version
}

// The key named `_serialize` of InvokeRequest.Metadata means that it is have been serialized
// Invoke do subscribe to dubbo service
func (ds *DUBBOSubscriber) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	interfaceName := req.Metadata[MetadataRpcInterface]
	version := req.Metadata[MetadataRpcVersion]
	group := req.Metadata[MetadataRpcGroup]
	ds.appendGenericAndProxyService(interfaceName, version, group, true)
	return nil, nil
}

// Operations return kind list with CreateOperation
func (ds *DUBBOSubscriber) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}
