package beakdingtalk

import "testing"

type fakeAPI struct {
	channel Channel
}

func (f *fakeAPI) RegisterChannel(channel Channel) error {
	f.channel = channel
	return nil
}

func TestRegister(t *testing.T) {
	api := &fakeAPI{}
	if err := Register(api); err != nil {
		t.Fatal(err)
	}
	if api.channel.Metadata().ID != ID {
		t.Fatalf("metadata=%+v", api.channel.Metadata())
	}
}
