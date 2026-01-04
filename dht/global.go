package dht

import (
	"context"
	"fmt"
	"net"
	"sync"
)

var globalDHT *BSDHT
var globalBSDHTMutex sync.Mutex
var globalRepoPath *string
var globalRepoMutex sync.Mutex

func SetGlobalBSDHT(dht *BSDHT) {
	globalBSDHTMutex.Lock()
	defer globalBSDHTMutex.Unlock()
	globalDHT = dht
}

func GetGlobalBSDHT() interface{} {
	globalBSDHTMutex.Lock()
	defer globalBSDHTMutex.Unlock()
	return globalDHT
}

func IsGlobalBSDHTDetected() bool {
	globalBSDHTMutex.Lock()
	defer globalBSDHTMutex.Unlock()
	return globalDHT != nil
}

func SetGlobalRepoPath(path *string) {
	globalRepoMutex.Lock()
	defer globalRepoMutex.Unlock()
	globalRepoPath = path
}

func GetGlobalRepoPath() *string {
	globalRepoMutex.Lock()
	defer globalRepoMutex.Unlock()
	return globalRepoPath
}

func (dht *BSDHT) LookupIPAddr(ctx context.Context, name string) ([]net.IPAddr, error) {
	fmt.Printf("*** LookupIPAddr %s\n", name)
	return net.DefaultResolver.LookupIPAddr(ctx, name)
}

func (dht *BSDHT) LookupTXT(ctx context.Context, name string) (values []string, err error) {
	log.Debugf("***LookupTXT %s\n", name)
	_, values, err = dht.LookupNames(ctx, name, false)
	return values, err
}
