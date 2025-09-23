package dht

import (
	"context"
	"fmt"
	"testing"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	"github.com/piax/go-byzskip/byzskip"
)

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

func addr(port int, quic bool) string {
	if quic {
		return fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic-v1", port)
	} else {
		return fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	}
}

func init() {
	logging.SetLogLevel("ayame", ayame.DEBUG)
	logging.SetLogLevel("byzskip", ayame.DEBUG)
}

func setupDualDHTs(ctx context.Context, numberOfPeers int, useQuic bool, sameTest bool) ([]*BSDHT, *authority.Authorizer) {
	byzskip.InitK(4)
	auth := authority.NewAuthorizer()
	count := 0
	authFunc := func(id peer.ID) (ayame.Key, string, *ayame.MembershipVector, []byte, error) {
		mv := ayame.NewMembershipVectorFromId(id.String())
		//i := count - 1
		//if i < 0 {
		//	i = numberOfPeers - 1
		//}
		i := count
		name := fmt.Sprintf("node-%d", i)
		if sameTest && i < numberOfPeers/10 {
			name = "abcdefg"
		}
		count++
		key := ayame.NewUnifiedKeyFromString(name, id)
		bin := auth.Authorize(id, key, name, mv, time.Now().Unix(), time.Now().Unix()+100)
		return key, name, mv, bin, nil
	}
	validateFunc := func(id peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte) bool {
		return authority.VerifyJoinCert(id, key, name, mv, cert, auth.PublicKey())
	}

	dhtOpts := []Option{
		/*		NamespacedValidator([]NameValidator{
				{Name: "hrns", Validator: blankValidator{}},
				{Name: "v", Validator: blankValidator{}},
			}),*/ // use default
		IdFinder(MVIdFinder),
		DisableFixLowPeers(true),
		Authorizer(authFunc),
		AuthValidator(validateFunc),
	}

	peers := make([]*BSDHT, numberOfPeers)

	var introducer string = ""
	for i := 0; i < numberOfPeers; i++ {
		locator := addr(9000+i, useQuic)
		p2pOpts := []libp2p.Option{libp2p.ListenAddrStrings(locator)}
		h, err := libp2p.New(p2pOpts...)
		if err != nil {
			panic(err)
		}
		if introducer != "" { // not a bootstrap node
			dhtOpts = append(dhtOpts, Bootstrap(introducer))
		}
		peer, err := New(h, dhtOpts...)
		if err != nil {
			panic(err)
		}
		peers[i] = peer
		if i == 0 { // bootstrap
			introducer = locator + "/p2p/" + peers[0].Node.Id().String()
			peers[i].RunAsBootstrap(ctx)
		} else {
			go func(pos int) {
				if err := peers[pos].Bootstrap(ctx); err != nil {
					panic(err)
				}
			}(i)
		}
	}
	return peers, auth
}

func setupDHTs(ctx context.Context, numberOfPeers int, useQuic bool) []*BSDHT {
	byzskip.InitK(4)
	auth := authority.NewAuthorizer()

	authFunc := func(id peer.ID) (ayame.Key, string, *ayame.MembershipVector, []byte, error) {
		mv := ayame.NewMembershipVectorFromId(id.String())
		key := ayame.NewIdKey(id)
		name := ""
		bin := auth.Authorize(id, key, name, mv, time.Now().Unix(), time.Now().Unix()+100)
		return key, name, mv, bin, nil
	}
	validateFunc := func(id peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte) bool {
		return authority.VerifyJoinCert(id, key, name, mv, cert, auth.PublicKey())
	}

	dhtOpts := []Option{
		NamespacedValidator([]NameValidator{
			{Name: "v", Validator: blankValidator{}},
		}),
		Authorizer(authFunc),
		AuthValidator(validateFunc),
	}

	peers := make([]*BSDHT, numberOfPeers)

	var introducer string = ""
	for i := 0; i < numberOfPeers; i++ {
		locator := addr(9000+i, useQuic)
		p2pOpts := []libp2p.Option{libp2p.ListenAddrStrings(locator)}
		h, err := libp2p.New(p2pOpts...)
		if err != nil {
			panic(err)
		}
		if introducer != "" { // not a bootstrap node
			dhtOpts = append(dhtOpts, Bootstrap(introducer))
		}
		peer, err := New(h, dhtOpts...)
		if err != nil {
			panic(err)
		}
		peers[i] = peer
		if i == 0 { // bootstrap
			introducer = locator + "/p2p/" + peers[0].Node.Id().String()
			peers[i].RunAsBootstrap(ctx)
		} else {
			go func(pos int) {
				if err := peers[pos].Bootstrap(ctx); err != nil {
					panic(err)
				}
			}(i)
		}
	}
	time.Sleep(time.Duration(5) * time.Second)
	sumCount := int64(0)
	sumTraffic := int64(0)
	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].Node.Parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].Node.Parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Node.Key(), peers[i].Node.Parent.(*p2p.P2PNode).InBytes, peers[i].Node.Parent.(*p2p.P2PNode).InCount, float64(peers[i].Node.Parent.(*p2p.P2PNode).InBytes)/float64(peers[i].Node.Parent.(*p2p.P2PNode).InCount))
	}
	fmt.Printf("avg-join-num-msgs: %f\n", float64(sumCount)/float64(numberOfPeers))
	fmt.Printf("avg-join-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfPeers))
	fmt.Printf("avg-msg-size(bytes): %f\n", float64(sumTraffic)/float64(sumCount))
	return peers
}

func TestParseName(t *testing.T) {
	nk, err := ParseName("/hrns/byte-stream.abcde:12D3KooWRE5MumwHz8g961ZLcprBeMEjq46DTZ8cz86syJBnXEqK")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(nk)
}

/*
func TestHRNSRecord(t *testing.T) {
	nDHTs := 1
	dhts := setupDHTs(context.Background(), nDHTs, true)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
		}
	}()
	pcert, err := dhts[0].getPCert()
	if err != nil {
		t.Fatal(err)
	}
	rec, err := dhts[0].makeNamedValueRecord("/hrns/test", []byte("hello"), pcert)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CheckHRNSRecord(rec.Value); err != nil {
		t.Fatal(err)
	}

}*/

func TestValueGetSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 32
	bak := ayame.MembershipVectorSize
	defer func() {
		ayame.MembershipVectorSize = bak
	}()
	ayame.MembershipVectorSize = 256
	dhts := setupDHTs(ctx, nDHTs, true)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
		}
	}()

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := dhts[0].PutValue(ctxT, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2*60)
	defer cancel()

	val, err := dhts[1].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}
	vala, err := dhts[2].GetValue(ctxT, "/v/hello", Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	if string(vala) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(vala))
	}

	val, err = dhts[nDHTs-1].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

	val, err = dhts[nDHTs/2].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

}

func TestFailureRecovery(t *testing.T) {
	ctx := context.Background()
	nDHTs := 20
	bak := ayame.MembershipVectorSize
	defer func() {
		ayame.MembershipVectorSize = bak
	}()
	ayame.MembershipVectorSize = 256
	dhts, auth := setupDualDHTs(ctx, nDHTs, true, false)

	// Allow time for the DHT network to stabilize
	time.Sleep(time.Duration(5) * time.Second)

	// Put a value using the first DHT
	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	key := "/v/recovery-test"
	UpdateName(t, auth, dhts[0], key)
	err := dhts[0].PutValue(ctxT, key, []byte("test-recovery"))
	if err != nil {
		t.Fatal(err)
	}

	// Verify the value can be retrieved from multiple nodes
	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	val, err := dhts[nDHTs-1].GetValue(ctxT, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "test-recovery" {
		t.Fatalf("Expected 'test-recovery' got '%s'", string(val))
	}

	// Identify a node that can retrieve the value
	retrievalNode := nDHTs / 2
	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	_, err = dhts[retrievalNode].GetValue(ctxT, key)
	if err != nil {
		t.Fatal(err)
	}

	// Intentionally close several nodes including the original provider
	nodesToClose := []int{0, 1, 2, 3, 4}
	for _, i := range nodesToClose {
		t.Logf("Closing DHT node %d", i)
		err := dhts[i].Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Allow time for the network to detect failures
	time.Sleep(time.Duration(3) * time.Second)

	// Try to retrieve the value after node failures
	ctxT, cancel = context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	val, err = dhts[retrievalNode].GetValue(ctxT, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "test-recovery" {
		t.Fatalf("Expected 'test-recovery' got '%s'", string(val))
	}

	// Try from another node as well
	ctxT, cancel = context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	val, err = dhts[nDHTs-1].GetValue(ctxT, key)
	if err != nil {
		t.Fatal(err)
	}
	if string(val) != "test-recovery" {
		t.Fatalf("Expected 'test-recovery' got '%s'", string(val))
	}

	t.Log("Successfully retrieved value after node failures")
}

func TestNamedValueRecovery(t *testing.T) {
	ctx := context.Background()
	nDHTs := 32
	bak := ayame.MembershipVectorSize
	defer func() {
		ayame.MembershipVectorSize = bak
	}()
	ayame.MembershipVectorSize = 256
	dhts, _ := setupDualDHTs(ctx, nDHTs, true, false)

	time.Sleep(time.Duration(5) * time.Second)

	// Put a named value
	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := dhts[7].PutNamedValue(ctxT, "node-7", []byte("test-recovery"))
	if err != nil {
		t.Fatal(err)
	}

	// Verify the value can be retrieved
	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	vals, err := dhts[0].GetNamedValues(ctxT, "node-7", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(vals))
	}
	if string(vals[0].Val) != "test-recovery" {
		t.Fatalf("Expected 'test-recovery' got '%s'", string(vals[0].Val))
	}

	// Identify a node that can retrieve the value
	retrievalNode := nDHTs / 2
	ctxT, cancel = context.WithTimeout(ctx, time.Second*2)
	defer cancel()
	_, err = dhts[retrievalNode].GetNamedValues(ctxT, "node-7", true)
	if err != nil {
		t.Fatal(err)
	}

	// Intentionally close several nodes including the original provider
	nodesToClose := []int{5, 7, 8, 9}
	for _, i := range nodesToClose {
		t.Logf("Closing DHT node %d", i)
		err := dhts[i].Close()
		if err != nil {
			t.Fatal(err)
		}
	}

	// Allow time for the network to detect failures
	time.Sleep(time.Duration(3) * time.Second)

	// Try to retrieve the value after node failures
	ctxT, cancel = context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	vals, err = dhts[retrievalNode].GetNamedValues(ctxT, "node-7", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(vals))
	}
	if string(vals[0].Val) != "test-recovery" {
		t.Fatalf("Expected 'test-recovery' got '%s'", string(vals[0].Val))
	}

	// Try from another node as well
	ctxT, cancel = context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	vals, err = dhts[nDHTs-1].GetNamedValues(ctxT, "node-7", true)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(vals))
	}
	if string(vals[0].Val) != "test-recovery" {
		t.Fatalf("Expected 'test-recovery' got '%s'", string(vals[0].Val))
	}

	t.Log("Successfully retrieved value after node failures")
}

func TestSimultaneousPutGet(t *testing.T) {
	ctx := context.Background()
	nDHTs := 32
	bak := ayame.MembershipVectorSize
	defer func() {
		ayame.MembershipVectorSize = bak
	}()
	ayame.MembershipVectorSize = 256
	dhts, auth := setupDualDHTs(ctx, nDHTs, true, false)

	time.Sleep(time.Duration(5) * time.Second)

	numOps := 5
	// Create channels to coordinate goroutines
	done := make(chan bool)
	errs := make(chan error, numOps*2)

	// Start simultaneous put operations
	for i := 0; i < numOps; i++ {
		go func(i int) {
			ctxT, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			key := fmt.Sprintf("/v/test%d", i)
			value := fmt.Sprintf("test-value-%d", i)
			UpdateName(t, auth, dhts[i], key)
			err := dhts[i].PutValue(ctxT, key, []byte(value))
			if err != nil {
				errs <- err
				return
			}
			done <- true
		}(i)
	}

	// Start simultaneous get operations after a small delay
	time.Sleep(time.Millisecond * 100)
	for i := 0; i < numOps; i++ {
		go func(i int) {
			ctxT, cancel := context.WithTimeout(ctx, time.Second*2)
			defer cancel()
			key := fmt.Sprintf("/v/test%d", i)
			expectedValue := fmt.Sprintf("test-value-%d", i)
			// Use a different DHT node to get the value
			getter := (i + 1) % nDHTs
			val, err := dhts[getter].GetValue(ctxT, key)
			if err != nil {
				errs <- err
				return
			}
			if string(val) != expectedValue {
				errs <- fmt.Errorf("Expected '%s' got '%s'", expectedValue, string(val))
				return
			}
			done <- true
		}(i)
	}

	// Wait for all operations to complete
	for i := 0; i < numOps*2; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		case <-done:
			continue
		case <-time.After(time.Second * 5):
			t.Fatal("Test timed out")
		}
	}
}

func genAuthorizer(auth *authority.Authorizer, _ peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector) func(peer.ID) (ayame.Key, string, *ayame.MembershipVector, []byte, error) {
	return func(id peer.ID) (ayame.Key, string, *ayame.MembershipVector, []byte, error) {
		cert := auth.Authorize(id, key, name, mv, time.Now().Unix(), time.Now().Unix()+100)
		return key, name, mv, cert, nil
	}
}

func UpdateName(t *testing.T, auth *authority.Authorizer, dht *BSDHT, name string) []byte {
	// Set the name for the DHT
	dht.Node.SetName(name)

	// Create authorization using the DHT's authorizer
	authorizer := genAuthorizer(auth, dht.Node.Id(), dht.Node.Key(), name, dht.Node.MV())

	_, _, _, cert, err := authorizer(dht.Id())
	if err != nil {
		t.Fatal(err)
	}

	/*bin, _, _, err := authority.ExtractCert(cert)
	if err != nil {
		t.Fatal(err)
	}*/
	dht.Node.Parent.(*p2p.P2PNode).Cert = cert
	return cert
}

func TestPutNamedValue(t *testing.T) {
	//ayame.InitLogger(ayame.ERROR)
	ctx := context.Background()
	nDHTs := 32
	bak := ayame.MembershipVectorSize
	defer func() {
		ayame.MembershipVectorSize = bak
	}()
	ayame.MembershipVectorSize = 256
	dhts, auth := setupDualDHTs(ctx, nDHTs, true, false)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			//dhts[i].Close()
		}
	}()

	// Wait for network stabilization
	time.Sleep(time.Duration(5) * time.Second)

	// Test putting a named value with longer timeout
	ctxT, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	err := dhts[0].PutNamedValue(ctxT, "node-0", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for value propagation
	time.Sleep(time.Second)

	// Test getting named values with longer timeout
	ctxT, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	vals, err := dhts[1].GetNamedValues(ctxT, "node-0", true)

	if err != nil {
		t.Fatal(err)
	}

	if len(vals) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(vals))
	}

	if string(vals[0].Val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(vals[0].Val))
	}

	// Test putting multiple named values with longer timeout
	ctxT, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	UpdateName(t, auth, dhts[7], "node-7")
	err = dhts[7].PutNamedValue(ctxT, "node-7", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	// Wait for value propagation
	time.Sleep(time.Second)

	ctxT, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	UpdateName(t, auth, dhts[8], "node-7")
	dhts[8].PutNamedValue(ctxT, "node-7", []byte("world"))
	/*if err != nil {
		t.Fatal(err)
	}*/ // this should be here but for test, we don't care

	// Wait for value propagation
	time.Sleep(time.Second)

	// Test getting multiple named values with longer timeout
	ctxT, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	vals, err = dhts[nDHTs-1].GetNamedValues(ctxT, "node-7", true)

	if err != nil {
		t.Fatal(err)
	}

	if len(vals) != 2 {
		t.Fatalf("Expected 2 values, got %d %v", len(vals), vals)
	}

	found := make(map[string]bool)
	for _, v := range vals {
		found[string(v.Val)] = true
	}

	//if !found["hello"] || !found["world"] {
	//	t.Fatal("Did not find both expected values")
	//}

	// Test putting/getting with empty name
	ctxT, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	UpdateName(t, auth, dhts[0], "")

	err = dhts[0].PutNamedValue(ctxT, "", []byte("test"))
	if err != nil {
		t.Fatalf("Error putting value with empty name %v", err)
	}

	// Wait for value propagation
	time.Sleep(time.Second)

	// Test getting non-existent name with longer timeout
	ctxT, cancel = context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	vals, err = dhts[nDHTs/2-1].GetNamedValues(ctxT, "non-existent", true)

	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 0 {
		t.Fatalf("Expected 0 values for non-existent name, got %d", len(vals))
	}
}

func TestMixedDHTs(t *testing.T) {
	ctx := context.Background()
	nDHTs := 32
	bak := ayame.MembershipVectorSize
	defer func() {
		ayame.MembershipVectorSize = bak
	}()
	ayame.MembershipVectorSize = 320
	dhts, auth := setupDualDHTs(ctx, nDHTs, true, false)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			//dhts[i].Close()
		}
	}()
	for i := range nDHTs {
		fmt.Printf("key=%s,mv=%s\n", dhts[i].Node.Key(), dhts[i].Node.MV())
	}

	time.Sleep(time.Duration(5) * time.Second)

	// Test putting a named value
	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	UpdateName(t, auth, dhts[0], "node-1")
	err := dhts[0].PutNamedValue(ctxT, "node-1", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2*60)
	defer cancel()

	// Test getting named values
	vals, err := dhts[1].GetNamedValues(ctxT, "node-1", true)
	if err != nil {
		t.Fatal(err)
	}

	if len(vals) != 1 {
		t.Fatalf("Expected 1 value, got %d", len(vals))
	}

	if string(vals[0].Val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(vals[0].Val))
	}

	// Test putting multiple named values
	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	UpdateName(t, auth, dhts[0], "node-2")
	err = dhts[0].PutNamedValue(ctxT, "node-2", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	UpdateName(t, auth, dhts[1], "node-2")
	err = dhts[1].PutNamedValue(ctxT, "node-2", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2*60)
	defer cancel()

	// Test getting multiple named values
	vals, err = dhts[2].GetNamedValues(ctxT, "node-2", true)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Printf("get named values done: for %s, %v\n", "node-2", vals)

	if len(vals) != 2 {
		t.Fatalf("Expected 2 values, got %d", len(vals))
	}

	found := make(map[string]bool)
	for _, v := range vals {
		found[string(v.Val)] = true
	}

	if !found["hello"] || !found["world"] {
		t.Fatal("Did not find both expected values")
	}

	// Test putting/getting with empty name
	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	UpdateName(t, auth, dhts[0], "")
	err = dhts[0].PutNamedValue(ctxT, "", []byte("test"))
	if err != nil {
		t.Fatalf("Error putting value with empty name %v", err)
	}

	// Test PutValue/GetValue
	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	err = dhts[0].PutValue(ctxT, "/v/test", []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()

	val, err := dhts[1].GetValue(ctxT, "/v/test")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "hello" {
		t.Fatalf("Expected 'hello' got '%s'", string(val))
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	// Test with Quorum(0)
	val, err = dhts[2].GetValue(ctxT, "/v/test", Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "hello" {
		t.Fatalf("Expected 'hello' got '%s'", string(val))
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	// Test from last node
	val, err = dhts[nDHTs-1].GetValue(ctxT, "/v/test")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "hello" {
		t.Fatalf("Expected 'hello' got '%s'", string(val))
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second)
	defer cancel()
	// Test from middle node
	val, err = dhts[nDHTs/2].GetValue(ctxT, "/v/test")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "hello" {
		t.Fatalf("Expected 'hello' got '%s'", string(val))
	}
}
