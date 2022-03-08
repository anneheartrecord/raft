package main

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/lesismal/arpc"
	"github.com/xtaci/kcp-go"
	"golang.org/x/crypto/pbkdf2"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

var PublicKey = [...]byte{45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 112, 117, 98, 108, 105, 99, 32, 107, 101, 121, 45, 45, 45, 45, 45, 10, 77, 73, 73, 66, 73, 106, 65, 78, 66, 103, 107, 113, 104, 107, 105, 71, 57, 119, 48, 66, 65, 81, 69, 70, 65, 65, 79, 67, 65, 81, 56, 65, 77, 73, 73, 66, 67, 103, 75, 67, 65, 81, 69, 65, 51, 101, 113, 87, 90, 108, 109, 116, 68, 84, 70, 49, 72, 43, 68, 53, 97, 50, 84, 68, 10, 85, 55, 75, 112, 76, 115, 85, 117, 52, 90, 49, 101, 82, 122, 103, 101, 77, 120, 83, 109, 111, 74, 48, 48, 68, 107, 119, 122, 83, 108, 53, 78, 122, 90, 107, 71, 117, 104, 82, 80, 47, 75, 49, 82, 80, 72, 56, 102, 70, 87, 84, 106, 56, 120, 65, 101, 102, 74, 51, 107, 54, 56, 86, 73, 10, 43, 97, 47, 54, 122, 121, 71, 72, 119, 98, 73, 43, 97, 89, 57, 72, 85, 100, 50, 73, 111, 108, 66, 86, 72, 48, 108, 89, 89, 113, 56, 89, 116, 103, 52, 75, 109, 68, 121, 84, 80, 49, 78, 48, 111, 110, 101, 52, 117, 76, 114, 82, 66, 89, 105, 90, 49, 77, 81, 97, 113, 75, 68, 105, 10, 103, 78, 103, 118, 43, 110, 84, 81, 54, 110, 105, 48, 115, 102, 56, 104, 72, 82, 88, 83, 51, 79, 101, 66, 89, 105, 116, 74, 79, 79, 81, 76, 67, 88, 119, 89, 99, 114, 85, 81, 52, 55, 57, 77, 81, 68, 107, 82, 78, 66, 69, 79, 54, 84, 119, 107, 85, 82, 65, 113, 122, 110, 101, 86, 10, 53, 115, 83, 108, 51, 111, 83, 48, 69, 119, 70, 118, 73, 53, 108, 50, 55, 53, 75, 87, 84, 49, 87, 101, 86, 66, 97, 81, 108, 109, 67, 114, 83, 98, 120, 119, 86, 71, 102, 65, 117, 54, 114, 80, 107, 107, 85, 81, 83, 43, 52, 88, 85, 54, 86, 89, 74, 83, 72, 111, 97, 121, 84, 54, 10, 77, 80, 69, 107, 49, 55, 49, 108, 68, 88, 81, 99, 79, 114, 74, 98, 100, 79, 120, 67, 89, 114, 52, 90, 120, 105, 51, 55, 119, 43, 122, 113, 54, 110, 117, 101, 88, 81, 114, 103, 54, 53, 118, 100, 108, 107, 82, 74, 103, 102, 112, 56, 102, 51, 106, 121, 84, 74, 83, 100, 43, 97, 56, 108, 10, 78, 119, 73, 68, 65, 81, 65, 66, 10, 45, 45, 45, 45, 45, 69, 78, 68, 32, 112, 117, 98, 108, 105, 99, 32, 107, 101, 121, 45, 45, 45, 45, 45, 10}
var PrivateKey = [...]byte{45, 45, 45, 45, 45, 66, 69, 71, 73, 78, 32, 112, 114, 105, 118, 97, 116, 101, 32, 107, 101, 121, 45, 45, 45, 45, 45, 10, 77, 73, 73, 69, 112, 65, 73, 66, 65, 65, 75, 67, 65, 81, 69, 65, 51, 101, 113, 87, 90, 108, 109, 116, 68, 84, 70, 49, 72, 43, 68, 53, 97, 50, 84, 68, 85, 55, 75, 112, 76, 115, 85, 117, 52, 90, 49, 101, 82, 122, 103, 101, 77, 120, 83, 109, 111, 74, 48, 48, 68, 107, 119, 122, 10, 83, 108, 53, 78, 122, 90, 107, 71, 117, 104, 82, 80, 47, 75, 49, 82, 80, 72, 56, 102, 70, 87, 84, 106, 56, 120, 65, 101, 102, 74, 51, 107, 54, 56, 86, 73, 43, 97, 47, 54, 122, 121, 71, 72, 119, 98, 73, 43, 97, 89, 57, 72, 85, 100, 50, 73, 111, 108, 66, 86, 72, 48, 108, 89, 10, 89, 113, 56, 89, 116, 103, 52, 75, 109, 68, 121, 84, 80, 49, 78, 48, 111, 110, 101, 52, 117, 76, 114, 82, 66, 89, 105, 90, 49, 77, 81, 97, 113, 75, 68, 105, 103, 78, 103, 118, 43, 110, 84, 81, 54, 110, 105, 48, 115, 102, 56, 104, 72, 82, 88, 83, 51, 79, 101, 66, 89, 105, 116, 74, 10, 79, 79, 81, 76, 67, 88, 119, 89, 99, 114, 85, 81, 52, 55, 57, 77, 81, 68, 107, 82, 78, 66, 69, 79, 54, 84, 119, 107, 85, 82, 65, 113, 122, 110, 101, 86, 53, 115, 83, 108, 51, 111, 83, 48, 69, 119, 70, 118, 73, 53, 108, 50, 55, 53, 75, 87, 84, 49, 87, 101, 86, 66, 97, 81, 10, 108, 109, 67, 114, 83, 98, 120, 119, 86, 71, 102, 65, 117, 54, 114, 80, 107, 107, 85, 81, 83, 43, 52, 88, 85, 54, 86, 89, 74, 83, 72, 111, 97, 121, 84, 54, 77, 80, 69, 107, 49, 55, 49, 108, 68, 88, 81, 99, 79, 114, 74, 98, 100, 79, 120, 67, 89, 114, 52, 90, 120, 105, 51, 55, 10, 119, 43, 122, 113, 54, 110, 117, 101, 88, 81, 114, 103, 54, 53, 118, 100, 108, 107, 82, 74, 103, 102, 112, 56, 102, 51, 106, 121, 84, 74, 83, 100, 43, 97, 56, 108, 78, 119, 73, 68, 65, 81, 65, 66, 65, 111, 73, 66, 65, 71, 109, 88, 120, 104, 106, 78, 99, 116, 112, 107, 80, 105, 98, 57, 10, 70, 98, 70, 85, 77, 78, 48, 83, 108, 103, 51, 55, 74, 87, 79, 86, 84, 100, 98, 49, 106, 79, 107, 104, 87, 109, 102, 121, 90, 87, 75, 111, 80, 51, 43, 84, 102, 57, 72, 107, 106, 112, 55, 99, 75, 51, 77, 47, 120, 80, 102, 85, 82, 83, 89, 89, 54, 53, 52, 115, 102, 86, 85, 117, 10, 106, 69, 55, 106, 103, 79, 88, 79, 74, 114, 109, 73, 97, 117, 66, 118, 118, 43, 76, 97, 109, 107, 85, 74, 57, 73, 110, 72, 84, 55, 109, 106, 88, 76, 115, 89, 78, 100, 110, 113, 116, 72, 47, 98, 48, 118, 83, 43, 56, 84, 67, 100, 90, 118, 102, 47, 98, 74, 89, 70, 101, 97, 48, 84, 10, 52, 116, 116, 54, 77, 107, 121, 56, 107, 90, 102, 76, 72, 100, 49, 122, 115, 103, 100, 65, 76, 114, 70, 52, 81, 79, 49, 55, 70, 55, 65, 47, 79, 49, 118, 111, 66, 71, 77, 49, 119, 51, 117, 57, 76, 51, 79, 109, 65, 81, 74, 97, 50, 110, 88, 110, 67, 87, 119, 78, 89, 68, 104, 80, 10, 85, 78, 73, 119, 116, 85, 78, 118, 101, 56, 53, 78, 105, 70, 114, 120, 48, 97, 78, 104, 105, 81, 122, 66, 75, 88, 76, 106, 99, 118, 113, 79, 101, 113, 68, 55, 84, 78, 69, 70, 53, 81, 106, 108, 53, 108, 73, 108, 72, 82, 52, 69, 120, 113, 83, 81, 112, 81, 100, 51, 67, 82, 104, 78, 10, 75, 119, 89, 101, 121, 111, 73, 68, 65, 89, 73, 89, 54, 49, 118, 76, 87, 75, 89, 80, 112, 74, 105, 81, 65, 113, 121, 47, 108, 76, 105, 88, 99, 84, 108, 74, 118, 47, 121, 43, 50, 102, 77, 52, 81, 66, 104, 102, 117, 86, 106, 106, 74, 82, 99, 72, 49, 100, 107, 48, 85, 55, 74, 87, 10, 89, 74, 66, 82, 101, 119, 69, 67, 103, 89, 69, 65, 43, 101, 115, 116, 102, 54, 69, 104, 108, 79, 100, 115, 97, 100, 113, 107, 121, 43, 83, 118, 89, 114, 89, 116, 98, 110, 81, 71, 119, 104, 67, 78, 77, 68, 122, 122, 48, 43, 118, 49, 53, 90, 108, 113, 55, 47, 52, 78, 88, 112, 90, 89, 10, 122, 113, 82, 75, 50, 89, 98, 113, 121, 50, 88, 52, 100, 52, 108, 69, 56, 53, 86, 101, 57, 122, 80, 113, 51, 72, 110, 51, 87, 88, 102, 118, 73, 56, 79, 83, 82, 50, 104, 69, 76, 117, 82, 83, 104, 115, 103, 49, 105, 84, 49, 117, 76, 101, 65, 86, 57, 79, 99, 121, 119, 89, 106, 82, 10, 100, 113, 113, 52, 79, 113, 85, 111, 86, 97, 67, 50, 84, 52, 76, 65, 48, 79, 81, 100, 48, 82, 112, 65, 110, 89, 66, 97, 87, 88, 112, 106, 81, 55, 69, 109, 78, 75, 105, 119, 122, 54, 84, 54, 107, 89, 54, 89, 72, 121, 84, 54, 72, 51, 56, 67, 103, 89, 69, 65, 52, 49, 68, 53, 10, 102, 83, 47, 51, 81, 118, 73, 113, 114, 49, 54, 111, 105, 118, 43, 115, 107, 117, 108, 79, 121, 65, 119, 85, 83, 99, 122, 79, 89, 71, 85, 111, 78, 53, 120, 56, 72, 70, 53, 106, 67, 117, 68, 73, 108, 97, 87, 119, 65, 114, 81, 51, 65, 70, 81, 50, 89, 120, 121, 47, 79, 75, 119, 83, 10, 77, 100, 80, 71, 70, 115, 74, 67, 71, 67, 72, 78, 113, 118, 109, 79, 52, 120, 82, 116, 54, 49, 117, 71, 111, 75, 98, 74, 66, 120, 99, 103, 71, 74, 51, 118, 55, 104, 68, 111, 48, 90, 97, 43, 105, 117, 114, 70, 74, 55, 102, 87, 49, 115, 116, 107, 109, 114, 68, 118, 118, 98, 102, 86, 10, 89, 78, 112, 86, 90, 83, 101, 87, 88, 104, 112, 110, 51, 90, 111, 48, 72, 119, 77, 53, 114, 110, 71, 116, 113, 71, 69, 76, 70, 66, 120, 89, 66, 97, 118, 52, 49, 107, 107, 67, 103, 89, 69, 65, 53, 101, 102, 115, 88, 105, 110, 98, 50, 56, 67, 100, 89, 105, 43, 113, 82, 73, 116, 109, 10, 115, 74, 100, 73, 54, 103, 66, 65, 113, 48, 120, 81, 112, 104, 121, 99, 98, 99, 114, 50, 49, 98, 87, 120, 119, 51, 57, 104, 121, 121, 55, 105, 71, 89, 55, 88, 56, 73, 54, 70, 57, 111, 69, 105, 57, 115, 120, 72, 113, 120, 69, 120, 89, 74, 88, 90, 75, 43, 80, 106, 111, 79, 71, 78, 10, 69, 56, 104, 53, 67, 56, 109, 109, 86, 56, 68, 104, 68, 109, 54, 108, 109, 85, 47, 121, 121, 75, 56, 74, 89, 57, 100, 65, 81, 80, 118, 100, 80, 90, 53, 70, 104, 109, 87, 105, 50, 107, 114, 84, 68, 65, 103, 76, 50, 116, 55, 80, 99, 121, 111, 69, 89, 82, 47, 80, 102, 100, 114, 120, 10, 56, 68, 98, 75, 72, 56, 49, 68, 103, 110, 71, 82, 116, 69, 43, 75, 76, 55, 82, 122, 105, 72, 56, 67, 103, 89, 65, 53, 105, 117, 47, 57, 72, 114, 49, 75, 57, 120, 54, 84, 69, 106, 85, 56, 75, 111, 105, 51, 68, 57, 82, 102, 107, 53, 56, 51, 74, 49, 56, 43, 121, 53, 121, 111, 10, 112, 122, 90, 76, 119, 67, 68, 50, 83, 102, 121, 56, 49, 82, 112, 47, 78, 76, 117, 80, 109, 56, 72, 65, 121, 84, 65, 109, 50, 110, 103, 111, 111, 79, 70, 79, 78, 90, 75, 104, 112, 84, 122, 53, 65, 80, 43, 73, 119, 69, 48, 113, 47, 82, 112, 82, 90, 75, 99, 83, 52, 109, 109, 47, 10, 107, 89, 117, 66, 67, 107, 114, 70, 99, 53, 70, 56, 102, 80, 50, 51, 108, 106, 81, 79, 88, 54, 74, 120, 47, 107, 82, 83, 70, 69, 48, 120, 110, 86, 78, 115, 114, 100, 55, 69, 112, 52, 54, 80, 98, 69, 43, 120, 89, 104, 71, 104, 57, 83, 106, 117, 114, 78, 73, 118, 72, 120, 87, 120, 10, 75, 100, 49, 116, 56, 81, 75, 66, 103, 81, 68, 101, 48, 118, 102, 111, 56, 111, 74, 69, 121, 83, 113, 53, 106, 103, 102, 65, 66, 97, 79, 79, 99, 112, 109, 65, 103, 54, 102, 50, 57, 57, 78, 103, 122, 109, 84, 57, 78, 107, 106, 110, 89, 70, 55, 107, 106, 48, 86, 84, 88, 83, 83, 98, 10, 104, 111, 50, 117, 49, 114, 52, 117, 86, 48, 85, 66, 120, 120, 110, 55, 107, 88, 56, 104, 97, 84, 79, 90, 43, 101, 57, 68, 107, 57, 111, 57, 101, 81, 76, 55, 68, 112, 74, 108, 102, 55, 51, 76, 105, 48, 55, 78, 86, 90, 52, 56, 66, 70, 43, 90, 104, 69, 107, 79, 118, 83, 110, 121, 10, 69, 52, 66, 104, 52, 105, 90, 102, 90, 119, 73, 77, 54, 113, 74, 107, 47, 88, 118, 72, 104, 103, 71, 121, 53, 99, 70, 105, 67, 111, 50, 122, 90, 99, 87, 106, 109, 121, 98, 56, 66, 79, 109, 111, 71, 54, 97, 55, 84, 110, 121, 101, 70, 65, 61, 61, 10, 45, 45, 45, 45, 45, 69, 78, 68, 32, 112, 114, 105, 118, 97, 116, 101, 32, 107, 101, 121, 45, 45, 45, 45, 45, 10}
var hostPorts = [][]string{{"127.0.0.1", "7000"}, {"127.0.0.1", "7001"}, {"127.0.0.1", "7002"}, {"127.0.0.1", "7003"}, {"127.0.0.1", "7004"}, {"127.0.0.1", "7005"}, {"127.0.0.1", "7006"}, {"127.0.0.1", "7007"}, {"127.0.0.1", "7008"}, {"127.0.0.1", "7009"}}

var l sync.Mutex

type node struct {
	status         int
	name           string
	host           string
	serverPort     string
	heartTime      time.Time
	heartTimeScale time.Duration
	outTime        time.Duration
	randTime       int
	term           int
	ticket         int
	serverStatus   map[string]bool
}
type kcpRpcServer struct {
	server  *arpc.Server
	content string
	ipPort  string
}
type kcpRpcClient struct {
	client  *arpc.Client
	content string
	ipPort  string
}

var kcpRpcServerList []*kcpRpcServer
var kcpRpcClientList []*kcpRpcClient

func InitRpcServer() *kcpRpcServer {
	return new(kcpRpcServer)
}

type message struct {
	Data []byte
}

func (s *kcpRpcServer) StartServerRpc(content, ipPort string) *kcpRpcServer {
	s.content = content
	s.ipPort = ipPort
	kcpRpcServerList = append(kcpRpcServerList, s)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		key := pbkdf2.Key([]byte("xiaohuang"), []byte("xiaohuang"), 1024, 32, sha256.New)
		block, err := kcp.NewAESBlockCrypt(key)
		if err != nil {
			log.Fatalf("aes block crypt failed,%v", err)
		}
		ln, err := kcp.ListenWithOptions(ipPort, block, 10, 3)
		if err != nil {
			log.Fatalf("kcp listen failed,%v", err)
		}
		s.server = arpc.NewServer()
		wg.Done()
		err = s.server.Serve(ln)
		if err != nil {
			log.Fatalf("arpc serve failed,%v", err)
		}
	}()
	wg.Wait()
	return s
}

func (s *kcpRpcServer) AddHandle(url string, f func(message []byte) []byte) *kcpRpcServer {
	s.server.Handler.Handle(url, func(context *arpc.Context) {
		mes := ""
		err := context.Bind(&mes)
		if err != nil {
			log.Fatalf("context bind failed,%v\n", err)
		}
		mesTmp := new(message)
		err = json.Unmarshal([]byte(mes), mesTmp)
		if err != nil {
			log.Fatalf("json unmarshal failed,%v\n", err)
		}
		decrypt, err := RSADecrypt(mesTmp.Data, PrivateKey[:])
		if err != nil {
			log.Fatalf("rsa decrypt failed,%v\n", err)
		}
		encrypt, err := RSAEncrypt(f(decrypt), PublicKey[:])
		if err != nil {
			log.Fatalf("rsa encrypt failed,%v\n", err)
		}
		mesTmp.Data = encrypt
		marshal, err := json.Marshal(mesTmp)
		if err != nil {
			log.Fatalf("json marshal failed,%v\n", err)
		}
		err = context.Write(marshal)
		if err != nil {
			log.Fatalf("context write failed,%v\n", err)
		}
	})
	return s
}

func InitRpcClient() *kcpRpcClient {
	return new(kcpRpcClient)
}
func (s *kcpRpcClient) StartClientRpc(content, ipPort string) *kcpRpcClient {
	kcpRpcClientList = append(kcpRpcClientList, s)
	s.content = content
	s.ipPort = ipPort
	var err error
	s.client, err = arpc.NewClient(func() (net.Conn, error) {
		key := pbkdf2.Key([]byte("STSKJ"), []byte("STSKJ"), 1024, 32, sha256.New)
		block, _ := kcp.NewAESBlockCrypt(key)
		return kcp.DialWithOptions(ipPort, block, 10, 3)
	})
	if err != nil {
		panic(err)
	}
	return s
}
func (s *kcpRpcClient) Request(url, req string, timeout time.Duration) ([]byte, error) {
	rsp := ""
	encrypt, err := RSAEncrypt([]byte(req), PublicKey[:])
	if err != nil {
		log.Fatalf("rsa encypt failed,%v\n", err)
	}
	mesTmp := new(message)
	mesTmp.Data = encrypt
	marshal, err := json.Marshal(mesTmp) //直接传字符串可能会发生问题  我们把结构体序列化一遍
	if err != nil {
		log.Fatalf("json marshal failed,%v\n", err)
	}
	req = string(marshal)
	err = s.client.Call(url, &req, &rsp, timeout)
	if err != nil {
		log.Fatalf("client call failed,%v\n", err)
	}
	err = json.Unmarshal([]byte(rsp), mesTmp)
	if err != nil {
		log.Fatalf("json unmarshal failed,%v\n", err)
	}
	decrypt, err := RSADecrypt(mesTmp.Data, PrivateKey[:])
	if err != nil {
		log.Fatalf("rsa decrypt failed,%v\n", err)
	}
	return decrypt, err
}
func (n *node) init(name, host, serverPort string, heartTimeScale, outTime time.Duration, randTime int) *node {
	n = &node{
		name:           name,
		host:           host,
		serverPort:     serverPort,
		term:           0,
		ticket:         0,
		heartTime:      time.Now(),
		heartTimeScale: heartTimeScale,
		outTime:        outTime,
		randTime:       randTime,
	}

	InitRpcServer().StartServerRpc("", host+":"+serverPort).AddHandle("/heartMessage", func(message []byte) []byte {
		i, err := strconv.Atoi(string(message)[6:])
		if err != nil {
			log.Fatalf("strconv atoi failed,%v", err)
		}
		if i >= n.term {
			n.status = 0
			n.ticket = 0
			n.term = i
			n.heartTime = time.Now()
			return []byte("ok")
		} else {
			return []byte("error")
		}
	}).AddHandle("/ticketMessage", func(message []byte) []byte {
		log.Println("结点", n.name, "现在的term是:", string(message))
		v, err := strconv.Atoi(string(message))
		if err != nil {
			log.Fatalf("strconv atoi failed,%v", err)
		}
		if n.term > v {
			return []byte("no")
		}
		if n.status == 2 || n.status == 1 {
			return []byte("no")
		}
		return []byte("yes")
	})
	n.serverStatus = make(map[string]bool)
	go func() {
		heartSend := true
		for {
			if n.status == 2 && heartSend {
				heartSend = false
				go func() {
					n.term++
					for _, j := range hostPorts {
						j_ := j
						if !(j_[0] == host && j_[1] == serverPort) {
							go func() {
								rsp, err := InitRpcClient().StartClientRpc("0", j[0]+":"+j[1]).Request("/heartMessage", "heart|"+strconv.Itoa(n.term), time.Second*2)
								if err != nil {

									return
								}
								if string(rsp) == "ok" {
									l.Lock()
									n.serverStatus[j[0]+"|"+j[1]] = true
									l.Unlock()
								} else {
									l.Lock()
									n.serverStatus[j[0]+"|"+j[1]] = false
									l.Unlock()
									log.Println(j[0]+"|"+j[1], "已经超时惹")
								}
							}()
						}
					}
					time.Sleep(heartTimeScale)
					heartSend = true
				}()
			}
			if n.status == 0 {
				rand.Seed(time.Now().UnixNano())
				ti := n.outTime + time.Duration(n.randTime)*time.Millisecond
				if n.heartTime.Add(ti).Before(time.Now()) {
					log.Println("结点" + n.name + "leader已经超时啦")
					n.status = 1
				}
			}
			if n.status == 1 {
				n.term++
				n.ticket++
				wg := sync.WaitGroup{}
				for _, j := range hostPorts {
					j_ := j
					if !(j_[0] == host && j_[1] == serverPort) {
						wg.Add(1)
						go func() {
							rsp, err := InitRpcClient().StartClientRpc("1", j_[0]+":"+j_[1]).Request("/ticketMessage", strconv.Itoa(n.term), time.Second*2)
							if err != nil {
								log.Fatalf("init rpc client,%v\n", err)
								return
							}
							if string(rsp) == "yes" {
								n.ticket++
								log.Println(j_[0]+"1"+j_[1], "现在的票数为", n.ticket)
							}
							wg.Done()
						}()
					}
				}
				wg.Wait()
				if n.ticket >= (len(hostPorts)+1)/2 {
					n.status = 2
					n.ticket = 0
					log.Println("现在的leader是", n.name)
					continue
				}
				n.ticket = 0
				time.Sleep(n.heartTimeScale)
			}
		}
	}()
	return n
}
func main() {
	for i, port := range hostPorts {
		n1 := &node{}

		n1.init(strconv.Itoa(i), port[0], port[1], time.Second*2, time.Second*4, 0)
	}
	select {}
}
