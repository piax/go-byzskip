package ayame

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/op/go-logging"
)

var Log = logging.MustGetLogger("ayame")

func InitLogger(level logging.Level) {
	var logFmt = logging.MustStringFormatter(
		`%{color}%{level:.4s}%{color:reset} %{message}`,
	)
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, logFmt)

	// set log level
	backendLeveled := logging.AddModuleLevel(backendFormatter)
	backendLeveled.SetLevel(level, "")

	logging.SetBackend(backendLeveled)
}

func ReverseSlice(data interface{}) {
	value := reflect.ValueOf(data)
	valueLen := value.Len()
	for i := 0; i <= int((valueLen-1)/2); i++ {
		reverseIndex := valueLen - 1 - i
		tmp := value.Index(reverseIndex).Interface()
		value.Index(reverseIndex).Set(value.Index(i))
		value.Index(i).Set(reflect.ValueOf(tmp))
	}
}

func LessThanExists(lst []int, x int) bool {
	for _, v := range lst {
		if v < x {
			return false
		}
	}
	return true
}

func AppendIfMissing(slice []Key, i Key) []Key {
	for _, ele := range slice {
		if ele.Equals(i) {
			return slice
		}
	}
	return append(slice, i)
}

func SliceString[T fmt.Stringer](args []T) string {
	rval := make([]string, len(args))
	for i, x := range args {
		rval[i] = x.String()
	}
	return "[" + strings.Join(rval, ",") + "]"
}

func SliceStringOld(args interface{}) string {
	r := reflect.ValueOf(args)
	rval := make([]string, r.Len())
	for i := 0; i < r.Len(); i++ {
		rval[i] = r.Index(i).Interface().(fmt.Stringer).String()
	}
	return "[" + strings.Join(rval, ",") + "]"
}
