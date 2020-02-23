package main

import (
    "fmt"
    "math"
    "math/rand"
    "os"
    "strconv"
    "sync"
    "time"
)

const NaN = int32(math.MinInt32)


// Acceptor结构体，即BasicPaxos协议中的 Acceptor
//  - maxN: 收到的最大贿金
//  - acceptN + acceptV: 接受的提案中的贿金和值
//  - mutex: 保证Acceptor一次只处理一个请求
type Acceptor struct {
    maxN int32
    acceptN int32
    acceptV int32
    mutex sync.Mutex
}

func NewAcceptor() *Acceptor {
    return &Acceptor{maxN:NaN, acceptN:NaN, acceptV:NaN}
}

// BasicPaxos中第1阶段，Proposor对Acceptor发起Prepare/行贿，调用这个函数
func (this *Acceptor)Prepare(N int32) (bool, int32, int32) {

    this.mutex.Lock()
    defer this.mutex.Unlock()

    if this.maxN == NaN {
        this.maxN = N
        return true, NaN, NaN
    } else {
        if this.maxN < N {
            this.maxN = N
            return true, this.acceptN, this.acceptV
        } else {
            return false, NaN, NaN
        }
    }
}

// BasicPaxos中第2阶段，Proposor对Acceptor发起Accept/提案，调用这个函数
func (this *Acceptor)Accept(N int32, V int32) (bool) {

    this.mutex.Lock()
    defer this.mutex.Unlock()

    if N < this.maxN {
        return false
    } else {
        this.maxN = N
        this.acceptN = N
        this.acceptV = V
        return true
    }
}

func SleepRand() {
    time.Sleep(time.Duration(rand.Int() % 50) * time.Microsecond)
}

// BasicPaxos中，单个Proposor的行为在这个函数中进行
//  - 用多个协程运行这个函数，来模拟多个Proposor
//  - 为模拟丢包、乱序的情况：
//    - 每个步骤之间sleep随机时长
//    - 不对所有的Acceptor发送请求，而是随机选择Acceptor中的一个多数派，对这个多数派发送请求
func OneProposerFlow(acceptors []*Acceptor, ProposerId int32, ProposerCnt int32, proposerResults []int32, wg *sync.WaitGroup) {
    defer wg.Done()

    acceptorCnt := len(acceptors)
    if acceptorCnt < 3 || acceptorCnt%2 != 1 {
        fmt.Printf("[OneProposerFlow] invalid acceptorCnt: %v\n", acceptorCnt)
        return
    }

    N               := ProposerId + ProposerCnt
    V               := rand.Int31() % 2000
    replySuccCnt    := 0
    replyMaxAcceptN := NaN
    replyMaxAcceptV := NaN

    for {

        // 第1阶段
        for {
            SleepRand()
            fmt.Printf("[OneProposerFlow %v - stage1 - start] N:%v V:%v\n", ProposerId, N, V)
            N += ProposerCnt
            replySuccCnt = 0

            // 从所有的Acceptor中选择一个乱序的多数派，进行Prepare/行贿
            chosen := rand.Perm(acceptorCnt)
            chosen = chosen[: acceptorCnt/2+1 + rand.Int() % (acceptorCnt/2+1)]

            for _, v := range chosen {
                SleepRand()
                cur := acceptors[v]
                replyStatus, replyAcceptN, replyAcceptV := cur.Prepare(N)

                if replyStatus {
                    replySuccCnt += 1
                    if replyAcceptN != NaN && replyAcceptN > replyMaxAcceptN {
                        replyMaxAcceptN = replyAcceptN
                        replyMaxAcceptV = replyAcceptV
                    }
                }
            }

            // 如果对超过半数的 Acceptor Prepare/行贿成功，则跳转到第二阶段
            if replySuccCnt >= acceptorCnt/2 + 1 {
                if replyMaxAcceptV != NaN {
                    V = replyMaxAcceptV
                }

                fmt.Printf("[OneProposerFlow %v - stage1 - succ ] N:%v V:%v replySuccCnt:%v/%v\n", ProposerId, N, V, replySuccCnt, len(chosen))
                break
            } else {
                fmt.Printf("[OneProposerFlow %v - stage1 - fail ] N:%v V:%v replySuccCnt:%v/%v\n", ProposerId, N, V, replySuccCnt, len(chosen))
            }
        }

        // 第2阶段
        SleepRand()
        fmt.Printf("[OneProposerFlow %v - stage2 - start] N:%v V:%v\n", ProposerId, N, V)

        // 从所有的Acceptor中选择一个乱序的多数派，进行 Accept
        chosen := rand.Perm(acceptorCnt)
        chosen = chosen[: acceptorCnt/2+1 + rand.Int() % (acceptorCnt/2+1)]
        replySuccCnt = 0
        for _, v := range chosen {
            SleepRand()
            cur := acceptors[v]
            replyStatus := cur.Accept(N, V)

            if replyStatus {
                replySuccCnt += 1
            }
        }

        if replySuccCnt >= acceptorCnt/2 + 1 {
            fmt.Printf("[OneProposerFlow %v - stage2 - succ ] N:%v V:%v replySuccCnt:%v/%v\n", ProposerId, N, V, replySuccCnt, len(chosen))
            proposerResults[ProposerId] = V
            break
        } else {
            fmt.Printf("[OneProposerFlow %v - stage2 - fail ] N:%v V:%v replySuccCnt:%v/%v\n", ProposerId, N, V, replySuccCnt, len(chosen))
        }
    }

}


func testBasicPaxos(proposerCnt int32, acceptorCnt int32) bool {
    if acceptorCnt < 3 || acceptorCnt%2 != 1 {
        fmt.Printf("[testBasicPaxos] invalid acceptorCnt: %v\n", acceptorCnt)
        return false
    }

    if proposerCnt < 1 {
        fmt.Printf("[testBasicPaxos] invalid proposerCnt: %v\n", proposerCnt)
        return false
    }

    acceptors := make([]*Acceptor, acceptorCnt)
    for i := 0; i < len(acceptors); i++ {
        acceptors[i] = NewAcceptor()
    }

    proposerResults := make([]int32, proposerCnt)

    wg := sync.WaitGroup{}
    for i := int32(0); i < proposerCnt; i++ {
        wg.Add(1)
        go OneProposerFlow(acceptors, i, proposerCnt, proposerResults, &wg)
    }
    wg.Wait()

    // 检查算法是否正常运行
    // 如果正常运行，数组中的所有值应该是一样的
    fmt.Printf("[testBasicPaxos] proposerResults: %v\n", proposerResults)
    for i := int32(0); i < proposerCnt; i++ {
        if proposerResults[i] != proposerResults[0] {
            return false
        }
    }
    return true
}


func main() {

    if len(os.Args) < 3 {
        fmt.Printf("[main] Usage:%v proposerCnt acceptorCnt\n", os.Args[0])
        fmt.Printf("[main] Usage:%v proposerCnt acceptorCnt testCnt(default 10)\n", os.Args[0])
        return
    }

    fmt.Printf("[main] Start\n")
    rand.Seed(time.Now().Unix())

    a1, _ := strconv.Atoi(os.Args[1])
    a2, _ := strconv.Atoi(os.Args[2])
    proposerCnt := int32(a1)
    acceptorCnt := int32(a2)
    testCnt := 10
    if len(os.Args) >= 4 {
        testCnt, _ = strconv.Atoi(os.Args[3])
    }
    //fmt.Printf("[main] proposerCnt:%v acceptorCnt:%v\n", proposerCnt, acceptorCnt)

    wrongCnt := 0
    for i := 0; i < testCnt; i++ {
        ret := testBasicPaxos(proposerCnt, acceptorCnt)
        if !ret {
            wrongCnt += 1
        }
    }
    fmt.Printf("[main] testCnt:%v wrongCnt:%v\n", testCnt, wrongCnt)

    fmt.Printf("[main] End\n")
}
