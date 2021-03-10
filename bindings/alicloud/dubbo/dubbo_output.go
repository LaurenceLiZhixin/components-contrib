package dubbo

import (
	"bytes"
	"context"
	constant2 "github.com/dapr/components-contrib/bindings/alicloud/dubbo/constants"
	"strconv"
	"strings"

	"github.com/apache/dubbo-go/common/constant"

	"github.com/dapr/components-contrib/bindings/alicloud/dubbo/logger"

	"github.com/dapr/components-contrib/bindings"
	perrors "github.com/pkg/errors"

	_ "github.com/apache/dubbo-go/cluster/cluster_impl"
	_ "github.com/apache/dubbo-go/cluster/loadbalance"
	_ "github.com/apache/dubbo-go/common/proxy/proxy_factory"
	_ "github.com/apache/dubbo-go/filter/filter_impl"
	_ "github.com/apache/dubbo-go/protocol/dubbo"
	_ "github.com/apache/dubbo-go/registry/consul"
	_ "github.com/apache/dubbo-go/registry/nacos"
	_ "github.com/apache/dubbo-go/registry/protocol"
	_ "github.com/apache/dubbo-go/registry/zookeeper"
	"github.com/dapr/components-contrib/bindings/alicloud/dubbo/constants"
)

type DUBBOOutputBinding struct {
	created bool
	logger  logger.Logger
	timeout int64
}

var dubboBinding *DUBBOOutputBinding

func NewDUBBOOutput(logger logger.Logger) *DUBBOOutputBinding {
	if dubboBinding != nil && dubboBinding.created {
		return dubboBinding
	}
	dubboBinding = &DUBBOOutputBinding{
		created: true,
		logger:  logger,
	}
	return dubboBinding
}

func (out *DUBBOOutputBinding) Init(metadata bindings.Metadata) error {
	//var appName = "dapr-sidecar"
	var (
		subscriberServices = metadata.Properties["subscribers"]
		timeout            = metadata.Properties["timeout"]
	)
	//timeout config
	if len(timeout) != 0 {
		out.timeout, _ = strconv.ParseInt(timeout, 10, 64)
	}
	if out.timeout == 0 {
		out.timeout = 3000
	}

	if len(subscriberServices) == 0 {
		return nil
	}
	subscriberServiceSlice := strings.Split(subscriberServices, ",")
	for _, v := range subscriberServiceSlice {
		if len(v) == 0 {
			continue
		}
		interfaceName, version, group := recoverKey(v)
		dubboSubscriber.appendGenericAndProxyService(interfaceName, version, group, false)
	}
	return nil
}

// The key named `_serialize` of InvokeRequest.Metadata means that it is have been serialized
func (out *DUBBOOutputBinding) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var (
		finalResult *bindings.InvokeResponse
		err         error
		result      []byte
	)
	group := req.Metadata[constants.MetadataRpcGroup]
	interfaceName := req.Metadata[constants.MetadataRpcInterface]
	version := req.Metadata[constants.MetadataRpcVersion]
	methodName := req.Metadata[constants.MetadataRpcMethodName]
	argTypes := req.Metadata[constants.MetadataRpcMethodParamTypes]
	if !out.checkParam(interfaceName, group, version, methodName) {
		return nil, perrors.New("the param is illegal.")
	}
	if req.Metadata[constants.MetadataRpcPassThrough] == "true" {
		attachments := make(map[string]interface{}, 4)
		serializerName := req.Metadata[constants.MetadataRpcSerializationType]
		if len(serializerName) == 0 {
			return nil, perrors.New("the serializer type is invalid")
		}
		attachments[constant2.SERIALIZE_TYPE_KEY] = serializerName

		context := context.WithValue(context.Background(), constant.AttachmentKey, attachments)

		//var argumentType []byte
		var argumentTypeArray []string
		if len(argTypes) != 0 {
			argumentTypeArray = strings.Split(argTypes, ",")
		} else {
			argumentTypeArray = make([]string, 0)
		}

		// 默认false
		if req.Metadata[constants.MetadataRpcGeneric] == "false" {
			proxyService := dubboSubscriber.getProxyService(interfaceName, version, group) //proxyService left
			var data [][]byte
			if data, err = convertDataProtocol(req.Data); err != nil {
				return nil, err
			}
			if data == nil {
				data = [][]byte{}
			}
			if result, err = proxyService.ProxyInvokeWithBytes(context, methodName, argumentTypeArray, data); err != nil {
				finalResult = &bindings.InvokeResponse{}
				return finalResult, err
			}
			finalResult = &bindings.InvokeResponse{
				Data: result,
			}
			return finalResult, nil
		}
	}
	return finalResult, err
}

func (out*DUBBOOutputBinding)checkParam(interfaceName, group, version, methodName string) bool {
	if len(interfaceName) == 0 || len(group) == 0 || len(version) == 0 || len(methodName) == 0 {
		out.logger.Warnf("the param is illegal. %s %s:%s %s", group, interfaceName, group, methodName)
		return false
	}
	return true
}
func (out *DUBBOOutputBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.CreateOperation}
}
func convertDataProtocol(data []byte) ([][]byte, error) {
	buf := bytes.NewBuffer(data)
	plenTmp, _ := ReadInt32(buf)
	if plenTmp == 0 {
		return nil, nil
	}
	plen := int(plenTmp)
	if len(data) < (plen*4 + 4) {
		return nil, perrors.New("the data is illegal.")
	}
	lens := make([]int32, plen)
	totalLen := 4 + 4*plen
	for i := 0; i < plen; i++ {
		lens[i], _ = ReadInt32(buf)
		totalLen += int(lens[i])
	}
	if totalLen != len(data) {
		return nil, perrors.New("the data is illegal, the total length is wrong.")
	}
	d := make([][]byte, plen)
	for i := 0; i < plen; i++ {
		d[i] = make([]byte, lens[i])
		buf.Read(d[i])
	}
	return d, nil
}

