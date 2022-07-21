package ayame

import (
	"fmt"
	"math/rand"
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

type Equality[T any] interface {
	Equals(T) bool
}

func Exclude[T Equality[T]](lst []T, ex []T) []T {
	ret := []T{}
	for _, n := range lst {
		found := false
		for _, m := range ex {
			if n.Equals(m) {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, n)
		}
	}
	return ret
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

func PickRandomly[T any](arg []T) T {
	i := rand.Intn(len(arg))
	return arg[i]
}
