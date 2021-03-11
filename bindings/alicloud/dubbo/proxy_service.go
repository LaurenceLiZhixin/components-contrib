package dubbo

import (
	"context"

	"reflect"

	"github.com/apache/dubbo-go-hessian2/java_exception"
	"github.com/apache/dubbo-go/common/constant"
	"github.com/apache/dubbo-go/protocol"
	invocation_impl "github.com/apache/dubbo-go/protocol/invocation"
	"github.com/dapr/dapr/pkg/logger"
	perrors "github.com/pkg/errors"
)

// ProxyService uses for proxy invoke for service call
type ProxyService struct {
	invoker      protocol.Invoker
	referenceStr string
	logger       logger.Logger
}

// NewProxyService returns a ProxyService instance with target @referenceStr
func NewProxyService(referenceStr string, logger logger.Logger) *ProxyService {
	return &ProxyService{
		referenceStr: referenceStr,
		logger:       logger,
	}
}

// Reference gets referenceStr from GenericService
func (p *ProxyService) Reference() string {
	return p.referenceStr
}

// SetInvoker set invoker for ProxyService
func (p *ProxyService) SetInvoker(invoker protocol.Invoker) {
	p.invoker = invoker
}

// ProxyInvokeWithBytes change given params bytes to dubbo invocation, and invoke the invoker.
func (p *ProxyService) ProxyInvokeWithBytes(ctx context.Context, methodName string, argsTypes []string, args [][]byte) ([]byte, error) {
	if p.invoker == nil {
		return nil, perrors.New("the invoker is nil, you should invoker `WithProxy` and `Init` of HSFApiConsumerBean")
	}

	var (
		err    error
		inv    *invocation_impl.RPCInvocation
		inIArr []interface{}
		inVArr []reflect.Value
		reply  reflect.Value
		out    *[]byte
	)

	out = new([]byte)
	reply = reflect.ValueOf(out)
	invCtx := ctx
	inIArr = make([]interface{}, len(args))
	inVArr = make([]reflect.Value, len(args))

	for i, arg := range args {
		inIArr[i] = arg
		inVArr[i] = reflect.ValueOf(arg)
	}

	inv = invocation_impl.NewRPCInvocationWithOptions(invocation_impl.WithMethodName(methodName),
		invocation_impl.WithArguments(inIArr), invocation_impl.WithReply(reply.Interface()),
		invocation_impl.WithParameterValues(inVArr), invocation_impl.WithParameterTypeNames(argsTypes))

	generic := (inv.AttributeByKey(constant.GENERIC_KEY, "")) == "true"

	if argsTypes, err := SerializedArgTypes(inIArr, inv.ParameterTypeNames(), generic); err != nil {
		return nil, perrors.WithStack(err)
	} else {
		argsTypesStr := make([]string, 0, 4)
		for _, v := range argsTypes {
			argsTypesStr = append(argsTypesStr, string(v))
		}
		// set args type string to attachment
		inv.SetAttachments(constant.ParameterTypeKey, argsTypesStr)
	}

	// add user setAttachment. It is compatibility with previous versions.
	atm := invCtx.Value(constant.AttachmentKey)
	if m, ok := atm.(map[string]string); ok {
		for k, value := range m {
			inv.SetAttachments(k, value)
		}
	} else if m2, ok2 := atm.(map[string]interface{}); ok2 {
		// it is support to transfer map[string]interface{}. It refers to dubbo-java 2.7.
		for k, value := range m2 {
			inv.SetAttachments(k, value)
		}
	}

	// call invoker
	result := p.invoker.Invoke(invCtx, inv)
	if len(result.Attachments()) > 0 {
		invCtx = context.WithValue(invCtx, constant.AttachmentKey, result.Attachments())
	}

	err = result.Error()
	if err != nil {
		// the cause reason
		err = perrors.Cause(err)
		// if some error happened, it should be log some info in the seperate file.
		if throwabler, ok := err.(java_exception.Throwabler); ok {
			p.logger.Warnf("invoke service throw exception: %v , stackTraceElements: %v", err.Error(), throwabler.GetStackTrace())
		} else {
			p.logger.Warnf("result err: %v", err)
		}
	} else {
		p.logger.Debugf("[makeDubboCallProxy] result: %v, err: %v", result.Result(), err)
	}

	return *out, err
}
