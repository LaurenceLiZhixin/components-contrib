// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package dubbo

import (
	"reflect"
	"time"

	hessian "github.com/apache/dubbo-go-hessian2"
	"github.com/pkg/errors"
)

// SerializedArgTypes get java type from golang @parameters and @args
func SerializedArgTypes(args []interface{}, parameters []string, generic bool) ([][]byte, error) {
	if generic {
		return [][]byte{[]byte("java.lang.String"), []byte("[Ljava.lang.String;"), []byte("[Ljava.lang.Object;")}, nil
	}

	if len(args) == 0 {
		return make([][]byte, 0), nil
	}
	types := make([][]byte, len(args))
	//
	if len(parameters) == len(args) {
		for i, p := range parameters {
			types[i] = []byte(p)
		}
		return types, nil
	}

	for i, v := range args {
		t := javaType(v)
		if t == "" {
			return types, errors.Errorf("cannot get arg %#v type", args[i])
		}
		types[i] = []byte(t)
	}
	return types, nil
}

// javaType get java type name form @v
func javaType(v interface{}) string {
	if v == nil {
		return "V"
	}
	if pojo, ok := v.(hessian.POJO); ok {
		return pojo.JavaClassName()
	}

	switch v.(type) {
	// Serialized tags for base types
	case nil:
		return "void"
	case bool:
		return "bool"
	case []bool:
		return "[bool;"
	case byte:
		return "byte"
	case []byte:
		return "[byte;"
	case int8:
		return "byte"
	case []int8:
		return "[byte;"
	case int16:
		return "short"
	case []int16:
		return "[short;"
	case uint16: // Equivalent to Char of Java
		return "char"
	case []uint16:
		return "[char;"
	// case rune:
	//	return "C"
	case int:
		return "long"
	case []int:
		return "[long;"
	case int32:
		return "int"
	case []int32:
		return "[int;"
	case int64:
		return "long"
	case []int64:
		return "[long;"
	case time.Time:
		return "java.util.Date"
	case []time.Time:
		return "[Ljava.util.Date;"
	case float32:
		return "float"
	case []float32:
		return "[float;"
	case float64:
		return "double"
	case []float64:
		return "[double;"
	case string:
		return "java.lang.String"
	case []string:
		return "[Ljava.lang.String;"
	case []hessian.Object:
		return "[Ljava.lang.Object;"
	case map[interface{}]interface{}:
		// return  "java.util.HashMap"
		return "java.util.Map"

	//  Serialized tags for complex types
	default:
		t := reflect.TypeOf(v)
		if reflect.Ptr == t.Kind() {
			t = reflect.TypeOf(reflect.ValueOf(v).Elem())
		}
		switch t.Kind() {
		case reflect.Struct:
			return "java.lang.Object"
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Struct {
				return "[Ljava.lang.Object;"
			}
			// return "java.util.ArrayList"
			return "java.util.List"
		case reflect.Map: // Enter here, map may be map[string]int
			return "java.util.Map"
		default:
			return ""
		}
	}
}
