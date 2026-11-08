package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pranavb1234/beacon/internal/config"
)

// Tests that 3 consumer group members share 30 messages roughly equally via round-robin.
func TestConsumerGroup_ThreeMembersShareMessages(t *testing.T) {
	cfg := config.Default()
	m := NewManager(cfg, nil)
	topic := "group-share-topic"
	group := "worker-group"

	m1 := NewSubscriber("worker-1", 100)
	m1.GroupID = group
	m2 := NewSubscriber("worker-2", 100)
	m2.GroupID = group
	m3 := NewSubscriber("worker-3", 100)
	m3.GroupID = group

	require.NoError(t, m.AddSubscriber(topic, m1))
	require.NoError(t, m.AddSubscriber(topic, m2))
	require.NoError(t, m.AddSubscriber(topic, m3))

	totalMessages := 30
	for i := 1; i <= totalMessages; i++ {
		_, err := m.Publish(topic, []byte(fmt.Sprintf("task-%d", i)))
		require.NoError(t, err)
	}

	// Each worker should receive exactly 10 messages with 3 members round-robin
	assert.Equal(t, 10, len(m1.Send))
	assert.Equal(t, 10, len(m2.Send))
	assert.Equal(t, 10, len(m3.Send))

	seenSeqs := make(map[uint64]bool)
	for _, worker := range []*Subscriber{m1, m2, m3} {
		for len(worker.Send) > 0 {
			msg := <-worker.Send
			assert.False(t, seenSeqs[msg.Seq], "message %d delivered more than once in group", msg.Seq)
			seenSeqs[msg.Seq] = true
			m.Acknowledge(topic, worker.ID, msg.Seq)
		}
	}

	assert.Equal(t, totalMessages, len(seenSeqs))
	assert.Equal(t, uint64(totalMessages), m.GetGroupCommittedOffset(topic, group))
}

// Tests that when a group member disconnects, its in-flight messages are redistributed to remaining members.
func TestConsumerGroup_MemberLeavesMidStream_Redistribution(t *testing.T) {
	cfg := config.Default()
	cfg.AckTimeout = 5 * time.Second // long timeout so retry scanner does not fire during test
	m := NewManager(cfg, nil)
	topic := "redistribute-topic"
	group := "failover-group"

	p1 := NewSubscriber("proc-1", 50)
	p1.GroupID = group
	p2 := NewSubscriber("proc-2", 50)
	p2.GroupID = group

	require.NoError(t, m.AddSubscriber(topic, p1))
	require.NoError(t, m.AddSubscriber(topic, p2))

	// Publish message 1 -> dispatched to proc-1
	seq1, err := m.Publish(topic, []byte("urgent-order-1"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq1)

	// proc-1 receives message 1
	msg := <-p1.Send
	assert.Equal(t, uint64(1), msg.Seq)
	assert.Equal(t, 1, p1.InFlightCount())

	// proc-1 disconnects without acknowledging
	err = m.RemoveSubscriber(topic, "proc-1")
	require.NoError(t, err)

	// In-flight message 1 must be redistributed to proc-2
	select {
	case redistributed := <-p2.Send:
		assert.Equal(t, uint64(1), redistributed.Seq)
		assert.Equal(t, "urgent-order-1", string(redistributed.Payload))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for unacked message to be redistributed to proc-2")
	}
}

// Tests that when a group has 0 members, messages stay in ring buffer and are delivered when a member joins.
func TestConsumerGroup_GroupRejoin_CatchesUp(t *testing.T) {
	cfg := config.Default()
	cfg.BufferSizePerTopic = 50
	m := NewManager(cfg, nil)
	topic := "rejoin-topic"
	group := "billing-group"

	// Publish 5 messages when group has zero members
	for i := 1; i <= 5; i++ {
		_, err := m.Publish(topic, []byte(fmt.Sprintf("invoice-%d", i)))
		require.NoError(t, err)
	}

	assert.Equal(t, uint64(0), m.GetGroupCommittedOffset(topic, group))

	// Member joins the group
	sub := NewSubscriber("billing-1", 50)
	sub.GroupID = group
	require.NoError(t, m.AddSubscriber(topic, sub))

	// Member should catch up on all 5 messages
	for i := 1; i <= 5; i++ {
		select {
		case msg := <-sub.Send:
			assert.Equal(t, uint64(i), msg.Seq)
			assert.Equal(t, fmt.Sprintf("invoice-%d", i), string(msg.Payload))
			m.Acknowledge(topic, sub.ID, msg.Seq)
		case <-time.After(200 * time.Millisecond):
			t.Fatalf("timed out waiting for catch-up message %d", i)
		}
	}

	assert.Equal(t, uint64(5), m.GetGroupCommittedOffset(topic, group))
}

// Tests mixed broadcast and consumer group subscribers on the same topic.
func TestConsumerGroup_MixedBroadcastAndGroup(t *testing.T) {
	cfg := config.Default()
	m := NewManager(cfg, nil)
	topic := "hybrid-topic"

	// Broadcast subscriber (GroupID == "")
	broadcastSub := NewSubscriber("audit-logger", 50)
	broadcastSub.GroupID = ""
	require.NoError(t, m.AddSubscriber(topic, broadcastSub))

	// Consumer group subscribers (GroupID == "payment-processors")
	g1 := NewSubscriber("pay-1", 50)
	g1.GroupID = "payment-processors"
	g2 := NewSubscriber("pay-2", 50)
	g2.GroupID = "payment-processors"
	require.NoError(t, m.AddSubscriber(topic, g1))
	require.NoError(t, m.AddSubscriber(topic, g2))

	totalMessages := 10
	for i := 1; i <= totalMessages; i++ {
		_, err := m.Publish(topic, []byte(fmt.Sprintf("payment-%d", i)))
		require.NoError(t, err)
	}

	// 1. Broadcast subscriber receives all 10 messages
	assert.Equal(t, totalMessages, len(broadcastSub.Send))

	// 2. Consumer group members share the 10 messages (5 each)
	assert.Equal(t, 5, len(g1.Send))
	assert.Equal(t, 5, len(g2.Send))
}

