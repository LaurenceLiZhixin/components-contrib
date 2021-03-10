package dubbo

import (
	"context"
	dubboCommon "github.com/apache/dubbo-go/common"
	"sync"
	"time"

	"github.com/apache/dubbo-go/common/logger"
	dubboConfig "github.com/apache/dubbo-go/config"
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
}

func Key(interfaceName, version, group string) string {
	return group + "/" + interfaceName + ":" + version
}

func NewConsumerConfigAPIBean(ctx context.Context) *ConsumerConfigAPIBean {
	return &ConsumerConfigAPIBean{
		ReferenceConfig: *dubboConfig.NewReferenceConfigByAPI(),
		Inited:          false,
		rawData:         false,
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
	ccb.proxyService = NewProxyService(ccb.GetKey())
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

func (ddc *ConsumerConfigAPIBean) GetKey() string {
	return Key(ddc.InterfaceName, ddc.Version, ddc.Group)
}

func (ddc *ConsumerConfigAPIBean) GetProxyService() *ProxyService {
	return ddc.proxyService
}

func (ddc *ConsumerConfigAPIBean) Init() {
	if ddc.Inited {
		return
	}
	// 这里目前一定要comment，不然hsf会使用dubbo的registry，感觉hsf的又问题
	configConsumer()
	ddc.once.Do(func() {
		logger.Debugf("start to init DUBBOApiConsumerBean of service: %s\n", ddc.GetKey())
		ddc.Refer(ddc.genericService)
		if ddc.genericService != nil {
			ddc.Implement(ddc.genericService)
		}
		if ddc.Proxy && ddc.proxyService != nil {
			ddc.proxyService.SetInvoker(ddc.GetProxy().GetInvoker())
		}
		ddc.Inited = true
	})
}

func configConsumer() {
	var check = true
	consumerConfig := dubboConfig.ConsumerConfig{
		BaseConfig: dubboConfig.BaseConfig{
			ApplicationConfig:  &dubboConfig.ApplicationConfig{},
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
