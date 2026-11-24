// Command confidential-tinfoil-harness runs Tinfoil Chat conversations whose
// model turns and tool calls all go to enclaves it attests against a pinned repo.
// Command confidential-tinfoil-harness 运行 Tinfoil Chat 对话，其模型的 turn 和工具调用都发往它针对固定 repo 进行 attestation 的 enclave。
package main

import "github.com/tinfoilsh/confidential-tinfoil-harness/internal/server"

func main() {
	server.Run()
}
