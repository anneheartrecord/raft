package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"raft/tools"
	"strconv"
	"sync"
	"time"
)

//我们把ip和端口定死 一共有三个结点的ip和端口
var hostPorts = [][]string{{"127.0.0.1", "7000"}, {"127.0.0.1", "7001"}, {"127.0.0.1", "7002"}, {"127.0.0.1", "7003"}, {"127.0.0.1", "7004"}, {"127.0.0.1", "7005"}, {"127.0.0.1", "7006"}, {"127.0.0.1", "7007"}, {"127.0.0.1", "7008"}, {"127.0.0.1", "7009"}}

/*
结点: 分别有三种状态 自己的名字  自己的IP  客户端和服务端端口 设置的心跳时间和超时时间  所处的版本号 和收到的票数
*/
//var snowNode *snowflake.Node
//var err error
//var news = make([][]byte, 0)
//
//func Init(nodeId int64) {
//	snowNode, err = snowflake.NewNode(nodeId)
//	if err != nil {
//		fmt.Println(err)
//	}
//}
//func GetId() int64 {
//	return snowNode.Generate().Int64()
//}

var l sync.Mutex

//var leaderIp string

//type messageMes struct { //消息的id+切片数组
//	id       int64
//	mesSlice []*messageMesSlice
//}
//type messageMesSlice struct { //消息的切片信息
//	index  int
//	isCopy bool
//	ipPort string
//}
type node struct {
	status         int    //三种结点职责 leader==2 follower==0 candidate==1
	name           string //名字
	host           string
	serverPort     string    //服务端端口
	heartTime      time.Time //心跳时间
	heartTimeScale time.Duration
	outTime        time.Duration //超时时间
	randTime       int
	term           int //版本号
	ticket         int //票数
	serverStatus   map[string]bool
	//项目名 方法名 请求序号 分片总长度 分片序号 ip 端口
	//MAP[string]  string=项目名|方法名
	//[消息序号 分片总长度 分片序号 ip 端口]
	//messageStatus map[string][]*messageMes //map 消息的状态
}

func (n *node) init(name, host, serverPort string, heartTimeScale time.Duration, outTime time.Duration, randTime int) *node {
	//赋值
	n.name = name
	n.host = host
	n.serverPort = serverPort
	n.term = 0
	n.ticket = 0
	n.heartTime = time.Now()
	n.heartTimeScale = heartTimeScale
	n.outTime = outTime
	n.randTime = randTime
	//n.messageStatus = make(map[string][]*messageMes)
	//服务器端的接口
	InitRpcServer().StartRpc("", host+":"+serverPort).AddHandle("/heartMessage", func(message []byte) []byte { //心跳接口
		i, err := strconv.Atoi(string(message)[6:]) //拿到后六位  也就是发送过来的版本号
		if err != nil {
			log.Println("strconv atoi failed", err)
			return nil
		}
		fmt.Println("heartServer", i, n.term)

		//拿到版本号 比较版本号大小 写回给客户端
		if i >= n.term { //heartMessage发送的leader term和自己的term作比较
			if err != nil {
				log.Println("ctx write failed", err)
			}
			n.status = 0 //变为follower状态
			n.ticket = 0
			n.term = i
			n.heartTime = time.Now()
			return []byte("ok")
		} else {
			if err != nil {
				log.Println("ctx write failed", err)
			}
			return []byte("error")
		}
	}).AddHandle("/ticketMessage", func(message []byte) []byte { //投票接口
		log.Println("现在的term是", string(message))
		v, err := strconv.Atoi(string(message))
		if err != nil {
		}
		if n.term > v { //candidate 传过来的版本号比自己小 不投票
			if err != nil {
				fmt.Println("ctx write failed", err)
			}
			return []byte("no")
		}
		if n.status == 2 || n.status == 1 { //自己的状态为leader或者follower 不投票
			if err != nil {
				fmt.Println("ctx write failed", err)
			}
			return []byte("no")
		}
		if err != nil {
			fmt.Println("ctx write failed", err)
		}
		return []byte("yes") //排除以上两种情况 则可以投票
	})
	//}).AddHandle("/recvMessage", func(message []byte) []byte { //收到消息的接口
	//	f := struct {
	//		Project string
	//		Method  string
	//		Data    []byte
	//	}{}
	//	err := json.Unmarshal(message, &f) //把消息反序列化到f结构体里
	//	if err != nil {
	//		fmt.Println(err)
	//		return nil
	//	}
	//	n.SaveFile(f.Project, f.Method, GetId(), f.Data) //存文件
	//	return []byte("ok")
	//}).AddHandle("/saveMessage", func(message []byte) []byte { //保存消息到文件的接口
	//	type send struct {                                     //结构体字段分别为 项目名 方法名 Id 切片索引 数据实体
	//		ProjectName      string
	//		MethodName       string
	//		RequestMessageId int64
	//		SliceIndex       int
	//		Data             []byte
	//	}
	//	tmp := new(send)
	//	err := json.Unmarshal(message, tmp) //先反序列化
	//	if err != nil {
	//		fmt.Println(err)
	//		return nil
	//	}
	//	//创建包含 项目名 方法名 ID 切片索引的文件
	//	f, err := os.OpenFile("./Files/"+tmp.ProjectName+"|"+tmp.MethodName+"|"+strconv.FormatInt(tmp.RequestMessageId, 10)+"|"+strconv.Itoa(tmp.SliceIndex), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0744)
	//	_, err = f.Write(tmp.Data)
	//	//把数据实体写到文件里
	//	if err != nil {
	//		fmt.Println(err)
	//		return nil
	//	}
	//	err = f.Close()
	//	if err != nil {
	//		fmt.Println(err)
	//		return nil
	//	}
	//	return []byte("ok")
	//}).AddHandle("/messageInfo", func(message []byte) []byte { //同步消息状态
	//	type info struct {
	//		M map[string][]*messageMes
	//	}
	//	msginfo := new(info)
	//	err := json.Unmarshal(message, msginfo) //反序列化到msginfo结构体中
	//	if err != nil {
	//		log.Println("json unmarshal failed", err)
	//		return nil
	//	}
	//	var result string
	//	for k, v := range msginfo.M { //k 是 string(projectname+methodname) v是[] *MessageMes
	//		result = result + k    //把map的string先放到result里面
	//		for _, v1 := range v { //k1是[] *MessageMes的索引 v1是 id + [] MessageMesSlice
	//			result += strconv.FormatInt(v1.id, 10) //把id放进去
	//			for _, v2 := range v1.mesSlice {
	//				result += strconv.Itoa(v2.index) + strconv.FormatBool(v2.isCopy) + v2.ipPort //至此我们把info 结构体全变成了string
	//			}
	//		}
	//	}
	//	return []byte(result) //同步消息结构
	//	//1.同步消息结构2.主和备份的算法3.后端拿消息，订阅-发布
	//	//什么是订阅-发布模式
	//	//只要发布者发布数据 订阅者可以收听它们
	//}).AddHandle("/getMessage", func(message []byte) []byte { //拿文件的接口
	//	type get struct {
	//		Data []byte
	//	}
	//	tmp := new(get)
	//	err := json.Unmarshal(message, tmp)
	//	if err != nil {
	//		log.Println("json unmarshal failed", err)
	//		return nil
	//	}
	//	n.StitchFiles(tmp.Data)
	//	return []byte("ok")
	//}).AddHandle("/stitchMessage", func(message []byte) []byte { //把不同消息缝合的接口
	//	type bigMes struct {
	//		ProjectName string
	//		MethodName  string
	//		MessageID   int64
	//		Index       int
	//		Data        []byte
	//	}
	//	tmp := new(bigMes)
	//	err := json.Unmarshal(message, tmp)
	//	if err != nil {
	//		log.Println("json unmarshal failed,err:", err)
	//		return nil
	//	}
	//	f, err := os.OpenFile(".Files/"+tmp.ProjectName+tmp.MethodName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0744)
	//	if err != nil {
	//		log.Println("os openFile failed", err)
	//		return nil
	//	}
	//	defer f.Close()
	//	_, err = f.Write(tmp.Data)
	//	if err != nil {
	//		log.Println("file write failed", err)
	//	}
	//	return []byte("ok")
	//})
	n.serverStatus = make(map[string]bool) //保存结点状态
	go func() {
		heartSend := true
		for {
			if n.status == 2 && heartSend { //如果结点是leader
				heartSend = false
				go func() {
					n.term++
					for _, j := range hostPorts {
						j_ := j
						if !(j[0] == host && j[1] == serverPort) { //如果是本身
							go func() { //其他的follower请求leader的心跳
								rsp, err := tools.InitRpcClient().StartRpcClient("0", j_[0]+":"+j_[1]).Request("/heartMessage", "heart|"+strconv.Itoa(n.term), time.Second*2) //给每个结点发送了leader的term
								if err != nil {
									return
								}
								if string(rsp) == "ok" { //判断leader是否超时 没超时
									l.Lock()
									n.serverStatus[j[0]+"|"+j[1]] = true
									l.Unlock()
								} else {
									l.Lock()
									n.serverStatus[j[0]+"|"+j[1]] = false
									l.Unlock()
									log.Println(j[0]+"|"+j[1], "已经超时了")
								}
							}()
						}
					}
					time.Sleep(heartTimeScale)
					heartSend = true
				}()

			}
			if n.status == 0 { //如果是follower 那么只干一件事 如果leader超时 立马把自己的状态转变成candidate
				rand.Seed(time.Now().UnixNano())
				ti := n.outTime + time.Duration(n.randTime)*time.Millisecond
				if n.heartTime.Add(ti).Before(time.Now()) { //leader超时了
					log.Println("leader已经超时啦")
					n.status = 1
				}

			}
			if n.status == 1 { //如果是candidate
				//投完票了
				n.term += 1
				n.ticket += 1 //先把自己版本号加一 给自己投一票
				wg := sync.WaitGroup{}
				for _, j := range hostPorts {
					j_ := j
					if !(j[0] == host && j[1] == serverPort) { //如果是本身
						wg.Add(1)
						go func() {
							//给其他的结点发请求 请它们给自己投票
							rsp, err := tools.InitRpcClient().StartRpcClient("1", j_[0]+":"+j_[1]).Request("/ticketMessage", strconv.Itoa(n.term), time.Second*2)
							if err != nil {
								fmt.Println(err)
								return
							}
							if string(rsp) == "yes" {
								n.ticket += 1
								log.Println(j[0]+"|"+j[1], "现在的票数为", n.ticket)
							}
							wg.Done()
						}()
					}
				}
				wg.Wait()
				//一轮投票结束之后 判断自己的票数是否超了一半  如果超过了则立马更新自己的状态为leader continue循环
				if n.ticket >= (len(hostPorts)+1)/2 {
					n.status = 2
					n.ticket = 0
					log.Println("现在的leader是", n.name)
					//leaderIp = n.host + ":" + n.serverPort
					//getLeader()
					continue
				}
				n.ticket = 0
				//没成功 把票数置0之后进行新一轮的投票
				time.Sleep(n.heartTimeScale)
			}
		}

	}()
	return n
}
func (n *node) SaveFile(project, interf string, messageId int64, data []byte) { // 项目名 方法名 ID 数据
	if n.status == 2 {                                                          //1.如果是leader
		usefulnode := make([]string, 0)
		for _, j := range hostPorts {
			j_ := j
			_, ok := n.serverStatus[j_[0]+"|"+j_[1]]
			if ok {
				usefulnode = append(usefulnode, j_[0]+"|"+j_[1])

			}
		}
		news = Burst(data, 100)
		if _, ok := n.messageStatus[project+"|"+interf]; !ok { //没有这个map则需要实例化
			n.messageStatus[project+"|"+interf] = make([]*messageMes, 0)
		}
		tmpMes := new(messageMes)
		tmpMes.id = messageId
		tmpMes.mesSlice = make([]*messageMesSlice, 0)
		allpercent := 0.0
		for i := 0; i < len(usefulnode); i++ {
			allpercent += GetDiskPercent(usefulnode[i])
		}
		for i, j := range news {
			tmpIndex := rand.Float64() * allpercent //两个备份结点取0-allpercent的随机数
			var tmpIndex_ float64
			for {
				tmpIndex_ = rand.Float64() * allpercent
				if tmpIndex_ != tmpIndex {
					break
				}
			}
			percent1 := 0.0
			percent2 := 0.0
			node1 := 0
			node2 := 0
			for i := 0; i < len(usefulnode); i++ {
				percent1 += GetDiskPercent(usefulnode[i])
				if percent1 >= tmpIndex { //根据磁盘百分比随机拿结点
					node1 = i
					break
				}
			}
			for i := 0; i < len(usefulnode); i++ {
				percent2 += GetDiskPercent(usefulnode[i])
				if percent2 >= tmpIndex_ {
					node2 = i
					break
				}
			}
			//定义一个send 结构体 把消息序列化到结构体里传过去
			type send struct {
				ProjectName      string
				MethodName       string
				RequestMessageId int64
				SliceIndex       int
				Data             []byte
			}
			sendMessageMain := new(send)
			sendMessageMain.ProjectName = project
			sendMessageMain.MethodName = interf
			sendMessageMain.SliceIndex = i
			sendMessageMain.RequestMessageId = messageId
			sendMessageMain.Data = j
			//进行赋值之后 然后序列化
			marshal, err := json.Marshal(sendMessageMain)
			if err != nil {
				fmt.Println(err)
				return
			}
			InitRpcClient().StartRpc("1", usefulnode[node1]).Request("/saveMessage", string(marshal), time.Second*2)
			InitRpcClient().StartRpc("1", usefulnode[node2]).Request("/saveMessage", string(marshal), time.Second*2)
			tmpMes_main := new(messageMesSlice)
			tmpMes_main.ipPort = usefulnode[node1]
			tmpMes_main.index = i
			tmpMes_main.isCopy = false
			tmpMes_copy := new(messageMesSlice)
			tmpMes_copy.ipPort = usefulnode[node2]
			tmpMes_copy.index = i
			tmpMes_copy.isCopy = true
			//消息的切片信息: 端口号 索引 是否备份
			tmpMes.mesSlice = append(tmpMes.mesSlice, tmpMes_main)
			tmpMes.mesSlice = append(tmpMes.mesSlice, tmpMes_copy)
			//消息的ID+信息切片合集
		}
		n.messageStatus[project+"|"+interf] = append(n.messageStatus[project+"|"+interf], tmpMes)
		//消息的项目名 方法名 + 切片【消息ID 切片信息】
	}
}
func main() {
	for i, port := range hostPorts {
		n1 := &node{}
		n1.init(strconv.Itoa(i), port[0], port[1], time.Second*2, time.Second*4, 0)
	}
	select {}
}
