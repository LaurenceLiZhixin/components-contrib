package dubbo

import (
	"bytes"
	"context"
	"strconv"
	"strings"

	"github.com/apache/dubbo-go/common/constant"

	"github.com/dapr/dapr/pkg/logger"

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
)

// DUBBOOutputBinding is impl of OutputBinding
type DUBBOOutputBinding struct {
	created  bool
	logger   logger.Logger
	timeout  int64
	metadata bindings.Metadata
}

var dubboBinding *DUBBOOutputBinding

// NewDUBBOOutput returns new DUBBOOutputBinding
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

// Init init DUBBOOutputBinding from @metadata
func (out *DUBBOOutputBinding) Init(metadata bindings.Metadata) error {
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
	out.metadata = metadata
	return nil
}

// The key named `_serialize` of InvokeRequest.Metadata means that it is have been serialized
// Invoke try to call dubbo invoker with bytes data, the invoker should be cached by dubbo subscriber
func (out *DUBBOOutputBinding) Invoke(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	var (
		finalResult *bindings.InvokeResponse
		err         error
		result      []byte
	)

	// get dubbo invocation params form req
	group := req.Metadata[MetadataRpcGroup]
	interfaceName := req.Metadata[MetadataRpcInterface]
	version := req.Metadata[MetadataRpcVersion]
	methodName := req.Metadata[MetadataRpcMethodName]
	argTypes := req.Metadata[MetadataRpcMethodParamTypes]

	if !out.checkParam(interfaceName, group, version, methodName) {
		return nil, perrors.New("the param is illegal.")
	}
	if req.Metadata[MetadataRpcPassThrough] == "true" {
		attachments := make(map[string]interface{}, 4)
		serializerName := req.Metadata[MetadataRpcSerializationType]
		if len(serializerName) == 0 {
			return nil, perrors.New("the serializer type is invalid")
		}
		attachments[SERIALIZE_TYPE_KEY] = serializerName

		// set attachments to new context
		ctx := context.WithValue(context.Background(), constant.AttachmentKey, attachments)

		var argumentTypeArray []string
		if len(argTypes) != 0 {
			argumentTypeArray = strings.Split(argTypes, ",")
		} else {
			argumentTypeArray = make([]string, 0)
		}

		// false default
		if req.Metadata[MetadataRpcGeneric] == "false" {
			proxyService := dubboSubscriber.getProxyService(interfaceName, version, group) //proxyService left
			var data [][]byte
			if data, err = convertDataProtocol(req.Data); err != nil {
				return nil, err
			}
			if data == nil {
				data = [][]byte{}
			}
			if result, err = proxyService.ProxyInvokeWithBytes(ctx, methodName, argumentTypeArray, data); err != nil {
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

func (out *DUBBOOutputBinding) checkParam(interfaceName, group, version, methodName string) bool {
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
