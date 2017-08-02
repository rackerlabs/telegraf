package remote_test

import (
	"testing"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/internal/config"
	"github.com/golang/mock/gomock"
	"github.com/influxdata/telegraf/remote"
	"fmt"
	"github.com/pkg/errors"
	_ "github.com/influxdata/telegraf/plugins/inputs/http_response"
	"time"
)

type greetingMatcher struct {
	region string
}

func (m greetingMatcher) Matches(x interface{}) bool {
	g, ok := x.(*remote.Greeting)
	if !ok {
		return false
	}

	if g == nil {
		return false
	}

	return g.Region == m.region
}

func (m greetingMatcher) String() string {
	return fmt.Sprintf("is greeting with region %v", m.region)
}

func TestAdd(t *testing.T) {
	config := config.NewConfig()
	config.Agent.Interval.Duration = 100*time.Millisecond
	agent, err := agent.NewAgent(config)
	if err != nil {
		t.Error(err)
	}

	shutdown := make(chan struct{}, 1)
	defer close(shutdown)

	go agent.Run(shutdown)

	c := remote.Connect(agent, "localhost", "west")
	if c == nil {
		t.FailNow()
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockTelegrafRemoteClient(ctrl)
	resp := NewMockTelegrafRemote_StartConfigStreamingClient(ctrl)

	client.EXPECT().
		StartConfigStreaming(gomock.Any(), &greetingMatcher{region:"west"}).
		Return(resp, nil)

	configPack := &remote.ConfigPack{
		New: []*remote.Config{
			{Id:"id-1", Definition:`[[inputs.http_response]]`},
		},
	}
	gomock.InOrder(
		resp.EXPECT().Recv().Return(configPack, nil),
		resp.EXPECT().Recv().Return(nil, errors.New("disconnect")),
	)

	err = c.RunWithClient(shutdown, client)
	if err == nil {
		t.Error("Expected disconnect error")
	}

	actual := make(chan []string, 1)
	// ensure agent go routine has a chance to process
	time.Sleep(200*time.Millisecond)
	agent.QueryManagedInputs(func(ids []string){
		actual <- ids
	})

	select {
		case ids := <- actual:
			t.Log("got ids", ids)
			if len(ids) != 1 {
				t.Error("Should have only been one input")
			}

		case <- time.After(15*time.Second):
			t.Error("timeout")
	}
}