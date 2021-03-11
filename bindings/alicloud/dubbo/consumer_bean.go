package dubbo

import (
	"sync"
	"time"

	dubboCommon "github.com/apache/dubbo-go/common"

	dubboConfig "github.com/apache/dubbo-go/config"
	"github.com/dapr/dapr/pkg/logger"
)

const (
	consulProtocol = "consul"
	nacosProtocol  = "nacos"
	zkProtocol     = "zookeeper"
	dubboProtocol  = "dubbo"
	dubboPort      = "20080"
)

type ConsumerConfigAPIBean struct {
	dubboConfig.ReferenceConfig
	genericService *dubboConfig.GenericService
	proxyService   *ProxyService
	Generic        bool
	Proxy          bool
	Inited         bool
	once           sync.Once
	rawData        bool
	logger         logger.Logger
}

func Key(interfaceName, version, group string) string {
	return group + "/" + interfaceName + ":" + version
}

func NewConsumerConfigAPIBean(logger logger.Logger) *ConsumerConfigAPIBean {
	return &ConsumerConfigAPIBean{
		ReferenceConfig: *dubboConfig.NewReferenceConfigByAPI(),
		Inited:          false,
		rawData:         false,
		logger:          logger,
	}
}

func (ccb *ConsumerConfigAPIBean) WithInterfaceName(interfaceName string) *ConsumerConfigAPIBean {
	ccb.InterfaceName = interfaceName
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithVersion(version string) *ConsumerConfigAPIBean {
	ccb.Version = version
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithGroup(group string) *ConsumerConfigAPIBean {
	ccb.Group = group
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithGeneric() *ConsumerConfigAPIBean {
	ccb.genericService = dubboConfig.NewGenericService(ccb.GetKey())
	dubboConfig.SetConsumerService(ccb.genericService)
	ccb.Generic = true
	return ccb
}
func (ccb *ConsumerConfigAPIBean) WithProxy() *ConsumerConfigAPIBean {
	ccb.proxyService = NewProxyService(ccb.GetKey(), ccb.logger)
	dubboConfig.SetConsumerService(ccb.proxyService)
	ccb.Proxy = true
	return ccb
}
func (ccb *ConsumerConfigAPIBean) WithLB(lb string) *ConsumerConfigAPIBean {
	ccb.Loadbalance = lb
	return ccb
}
func (ccb *ConsumerConfigAPIBean) WithCluster(cluster string) *ConsumerConfigAPIBean {
	ccb.Cluster = cluster
	return ccb
}
func (ccb *ConsumerConfigAPIBean) WithFilter(filter string) *ConsumerConfigAPIBean {
	ccb.Filter = filter
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithCheck(check bool) *ConsumerConfigAPIBean {
	ccb.Check = new(bool)
	*ccb.Check = check
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithParam(param map[string]string) *ConsumerConfigAPIBean {
	ccb.Params = param
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithProtocol(protocol string) *ConsumerConfigAPIBean {
	ccb.Protocol = protocol
	return ccb
}
func (ccb *ConsumerConfigAPIBean) WithRetries(retries string) *ConsumerConfigAPIBean {
	ccb.Retries = retries
	return ccb
}

func (ccb *ConsumerConfigAPIBean) WithRawData() *ConsumerConfigAPIBean {
	ccb.rawData = true
	return ccb
}

func (ccb *ConsumerConfigAPIBean) GetKey() string {
	return Key(ccb.InterfaceName, ccb.Version, ccb.Group)
}

func (ccb *ConsumerConfigAPIBean) GetProxyService() *ProxyService {
	return ccb.proxyService
}

func (ccb *ConsumerConfigAPIBean) Init() {
	if ccb.Inited {
		return
	}
	configConsumer()
	ccb.once.Do(func() {
		ccb.logger.Debugf("start to init DUBBOApiConsumerBean of service: %s\n", ccb.GetKey())
		ccb.Refer(ccb.genericService)
		if ccb.genericService != nil {
			ccb.Implement(ccb.genericService)
		}
		if ccb.Proxy && ccb.proxyService != nil {
			ccb.proxyService.SetInvoker(ccb.GetProxy().GetInvoker())
		}
		ccb.Inited = true
	})
}

func configConsumer() {
	var check = true
	consumerConfig := dubboConfig.ConsumerConfig{
		BaseConfig: dubboConfig.BaseConfig{
			ApplicationConfig: &dubboConfig.ApplicationConfig{},
		},
		ConnectTimeout: 3 * time.Second,
		RequestTimeout: 3 * time.Second,
		Check:          &check,
		Registries:     map[string]*dubboConfig.RegistryConfig{consulProtocol: configConsulRegister()},
		References:     make(map[string]*dubboConfig.ReferenceConfig, 32),
	}
	dubboConfig.SetConsumerConfig(consumerConfig)
}

func configZKRegister() *dubboConfig.RegistryConfig {
	return &dubboConfig.RegistryConfig{
		Protocol:   zkProtocol,
		TimeoutStr: "3s",
		Address:    "127.0.0.1:2181",
	}
}
func configConsulRegister() *dubboConfig.RegistryConfig {
	return &dubboConfig.RegistryConfig{
		Protocol:   consulProtocol,
		TimeoutStr: "3s",
		Simplified: true,
		Address:    "127.0.0.1:8500",
	}
}

func configNacosRegister() *dubboConfig.RegistryConfig {
	return &dubboConfig.RegistryConfig{
		Protocol:   nacosProtocol,
		TimeoutStr: "3s",
		Address:    "127.0.0.1:8848",
	}
}

func configProtocol() *dubboConfig.ProtocolConfig {
	return &dubboConfig.ProtocolConfig{
		Name: dubboProtocol,
		Port: dubboPort,
		Ip:   dubboCommon.GetLocalIp(),
	}
}
