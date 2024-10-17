package ctokenizer

import (
	"fmt"
	"math"
	"testing"

	"github.com/milvus-io/milvus/internal/util/tokenizerapi"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestTokenizer(t *testing.T) {

	testf := func(text string, tokenizer tokenizerapi.Tokenizer) {
		tokenStream := tokenizer.NewTokenStream(text)
		embeddingMap := map[uint32]float32{}
		hashMap := map[uint32]string{}
		defer tokenStream.Destroy()
		tokenCount := 0
		for tokenStream.Advance() {
			token := tokenStream.Token()
			hash := typeutil.HashString2Uint32(token)
			embeddingMap[hash] += 1
			hashMap[hash] = token
			tokenCount++
		}
		fmt.Printf("Total number of tokens: %d\n", tokenCount)
		fmt.Printf("Total number of unique tokens: %d\n", len(embeddingMap))
		fmt.Println("Tokens and their counts:")
		for hash, count := range embeddingMap {
			fmt.Printf("%s %d: %.0f\n", hashMap[hash], hash, count)
		}
	}

	// default tokenizer.
	{
		text := "The presence of communication amid scientific minds was equally important to the success of the Manhattan Project as scientific intellect was. The only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated."
		m := make(map[string]string)
		tokenizer, err := NewTokenizer(m)
		assert.NoError(t, err)
		defer tokenizer.Destroy()
		testf(text, tokenizer)
	}

	// jieba tokenizer.
	{
		text := "张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都有光明的前途 假若 做到 像 允许 充分 先后 先後 先生 全部 全面 兮 共同 关于 其 其一 其中 其二 其他 其余 其它 其实 其次"
		m := make(map[string]string)
		m["tokenizer"] = "jieba"
		tokenizer, err := NewTokenizer(m)
		assert.NoError(t, err)
		defer tokenizer.Destroy()
		testf(text, tokenizer)
	}

	x := math.NaN()

	fmt.Println(x < 0)
	fmt.Println(x == 0)
	fmt.Println(x > 0)
}
