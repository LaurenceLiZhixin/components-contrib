package dubbo

import (
	"context"
	"sync"

	dubboConstant "github.com/apache/dubbo-go/common/constant"

	"github.com/dapr/components-contrib/bindings/alicloud/dubbo/logger"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/bindings/alicloud/dubbo/constants"

	"strings"

	failback "github.com/dapr/components-contrib/bindings/alicloud/dubbo/failback"
)

const (
	product = "dapr"
	action  = "dubbo-subscribe"
)

var dubboSubscriber *DUBBOSubscriber

type DUBBOSubscriber struct {
	timeout       int64
	interfaceName string
	version       string
	group         string
	rpcServices   map[string]*ConsumerConfigAPIBean
	registerLock  sync.Mutex
	logger        logger.Logger
}

func NewDUBBOSubscriber(logger logger.Logger) *DUBBOSubscriber {
	dubboSubscriber = &DUBBOSubscriber{
		registerLock: sync.Mutex{},
		rpcServices:  make(map[string]*ConsumerConfigAPIBean, 4),
		logger:       logger,
	}
	return dubboSubscriber
}

func (ds *DUBBOSubscriber) Init(metadata bindings.Metadata) error {
	for _, item := range failback.FailBackState(product, action) {
		if len(item) == 0 {
			continue
		}
		interfaceName, version, group := recoverKey(item)
		ds.appendGenericAndProxyService(interfaceName, version, group, false)
	}
	return nil
}

func (ds *DUBBOSubscriber) getProxyService(interfaceName, version, group string) *ProxyService {
	key := key(interfaceName, version, group)
	rpcService := ds.rpcServices[key]
	if rpcService == nil {
		ds.appendGenericAndProxyService(interfaceName, version, group, true)
		rpcService = ds.rpcServices[key]
	}
	return rpcService.GetProxyService()
}

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
	consumerConfigAPI := NewConsumerConfigAPIBean(context.Background()).
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
		failback.StoreState(product, action, k)
	}

	ds.rpcServices[k] = consumerConfigAPI
}

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

func key(interfaceName, version, group string) string {
	return group + "/" + interfaceName + ":" + version
}

// The key named `_serialize` of InvokeRequest.Metadata means that it is have been serialized
func (ds *DUBBOSubscriber) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	interfaceName := req.Metadata[constants.MetadataRpcInterface]
	version := req.Metadata[constants.MetadataRpcVersion]
	group := req.Metadata[constants.MetadataRpcGroup]
	ds.appendGenericAndProxyService(interfaceName, version, group, true)
	return nil, nil
}

func (ds *DUBBOSubscriber) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}
