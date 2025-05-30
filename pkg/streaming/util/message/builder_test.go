package message_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestMutableBuilder(t *testing.T) {
	b := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithAllVChannel().
		MustBuildMutable()
	assert.True(t, b.IsPersisted())
	assert.Equal(t, b.VChannel(), "")

	b = message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{}).
		WithNotPersisted().
		WithVChannel("v1").
		MustBuildMutable()
	assert.False(t, b.IsPersisted())
	assert.Equal(t, b.VChannel(), "v1")

	assert.Panics(t, func() {
		message.NewCreateCollectionMessageBuilderV1().WithNotPersisted()
	})
}

func TestImmutableTxnBuilder(t *testing.T) {
	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithHeader(&message.BeginTxnMessageHeader{
			KeepaliveMilliseconds: 1000,
		}).
		WithBody(&message.BeginTxnMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable()
	msgID := walimplstest.NewTestMessageID(1)
	immutableBegin := begin.WithTimeTick(1).WithTxnContext(txnCtx).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

	b := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(immutableBegin))
	assert.NotZero(t, b.EstimateSize())
	assert.Greater(t, b.ExpiredTimeTick(), uint64(1))

	msg := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		WithVChannel("v1").
		MustBuildMutable()
	immutableMsg := msg.WithTimeTick(2).WithTxnContext(txnCtx).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	b.Add(immutableMsg)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable()
	immutableCommit := commit.WithTimeTick(3).WithTxnContext(txnCtx).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)

	assert.NotZero(t, b.EstimateSize())
	beginMsg, msgs := b.Messages()
	assert.NotEmpty(t, beginMsg)
	assert.Len(t, msgs, 1)
	b.Build(message.MustAsImmutableCommitTxnMessageV2(immutableCommit))
}
