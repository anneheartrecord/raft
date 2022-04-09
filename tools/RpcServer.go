package tools

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/lesismal/arpc"
	"github.com/xtaci/kcp-go"
	"golang.org/x/crypto/pbkdf2"
	"log"
	"net"
	"sync"
	"time"
)

var kcpRpcServerList []*kcpRpcServer //server列表
var kcpRpcClientList []*kcpRpcClient //client列表

//server结构体
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
type message struct {
	Data []byte
}

func init() { //初始化server列表和client列表
	kcpRpcServerList = make([]*kcpRpcServer, 0)
	kcpRpcClientList = make([]*kcpRpcClient, 0)
}
func InitRpcServer() *kcpRpcServer {
	return new(kcpRpcServer)
}
func InitRpcClient() *kcpRpcClient {
	return new(kcpRpcClient)
}
func (s *kcpRpcServer) StartRpcServer(content, ipPort string) *kcpRpcServer {
	kcpRpcServerList = append(kcpRpcServerList, s)
	s.content = content
	s.ipPort = ipPort
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		key := pbkdf2.Key([]byte("STSKJ"), []byte("STSKJ"), 1024, 32, sha256.New)
		//一个加密函数  传入password 和加盐 加密1024次 得到一个32位的[]byte 以sha256算法加密
		block, _ := kcp.NewAESBlockCrypt(key)
		ln, err := kcp.ListenWithOptions(ipPort, block, 10, 3)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s.server = arpc.NewServer()
		wg.Done()
		err = s.server.Serve(ln)
		if err != nil {
			fmt.Println(err)
		}

	}()
	wg.Wait()

	return s
	return s
	//1.使用AES加密拿到块
	//2.用块进行监听
	//3.创建一个server 赋值
	//添加到列表里
}
func (s *kcpRpcClient) StartRpcClient(content, ipPort string) *kcpRpcClient {
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
func (s *kcpRpcServer) AddHandle(url string, f func(message []byte) []byte) *kcpRpcServer {
	s.server.Handler.Handle(url, func(context *arpc.Context) {
		mes := ""
		err := context.Bind(&mes)
		if err != nil {
			fmt.Println(err)
			return
		}
		mesTmp := new(message)
		err = json.Unmarshal([]byte(mes), mesTmp)
		if err != nil {
			fmt.Println(err)
			return
		}
		decrypt, err := RSADecrypt(mesTmp.Data, PrivateKey[:])
		if err != nil {
			fmt.Println(err)
			return
		}
		encrypt, err := RSAEncrypt(f(decrypt), PublicKey[:])
		if err != nil {
			fmt.Println(err)
			return
		}
		mesTmp.Data = encrypt
		marshal, err := json.Marshal(mesTmp)
		if err != nil {
			fmt.Println(marshal)
			return
		}
		err = context.Write(marshal)
		if err != nil {
			fmt.Println(err)
			return
		}
	})
	return s
}
func (s *kcpRpcClient) Request(url string, req string, timeout time.Duration) ([]byte, error) {
	rsp := ""
	encrypt, err := RSAEncrypt([]byte(req), PublicKey[:])
	if err != nil {
		fmt.Println(err)
		return nil, nil
	}
	mesTmp := new(message)
	mesTmp.Data = encrypt
	marshal, err := json.Marshal(mesTmp)
	if err != nil {
		fmt.Println(marshal)
		return nil, nil
	}
	req = string(marshal)
	err = s.client.Call(url, &req, &rsp, timeout)
	if err != nil {
		return nil, nil
	}
	err = json.Unmarshal([]byte(rsp), mesTmp)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	decrypt, err := RSADecrypt(mesTmp.Data, PrivateKey[:])
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return decrypt, err
}
