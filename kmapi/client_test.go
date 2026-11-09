package kmapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv8/KeymasterAPIProtocol/kmapi"
)

func TestStatusIdentity(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/status" {
			_ = json.NewEncoder(w).Encode(map[string]any{"initialized": true, "state": "locked", "product": "km", "apiVersion": "v1"})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := kmapi.New(srv.URL, srv.Client())
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := kmapi.CheckIdentity(st); err != nil {
		t.Fatal(err)
	}
	if st.Product != "km" || st.APIVersion != "v1" {
		t.Fatalf("identity mismatch: %+v", st)
	}
}

func TestVersionMismatch(t *testing.T) {
	err := kmapi.CheckIdentity(kmapi.Status{Product: "other", APIVersion: "v1"})
	if err == nil {
		t.Fatal("expected version error")
	}
}

func TestHTTPErrorClassification(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/roots" && r.Method == http.MethodGet {
			http.Error(w, "尚未初始化", http.StatusPreconditionFailed)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := kmapi.New(srv.URL, srv.Client())
	_, err := c.ListRoots(context.Background())
	if !kmapi.IsNotInitialized(err) {
		t.Fatalf("expected 412, got %v", err)
	}
	if s, ok := kmapi.HTTPStatus(err); !ok || s != 412 {
		t.Fatalf("status = %v,%v", s, ok)
	}
}

func TestUnknownAPINotHTML(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 契约：未知 /api/* 返回 404，不返回 SPA HTML。
		if strings.HasPrefix(r.URL.Path, "/api/") {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html></html>"))
	}))
	defer srv.Close()
	c := kmapi.New(srv.URL, srv.Client())
	_, err := c.GetTask(context.Background(), strings.Repeat("a", 64))
	if !kmapi.IsNotFound(err) {
		t.Fatalf("expected 404, got %v", err)
	}
}

func TestTransportError(t *testing.T) {
	c := kmapi.New("http://127.0.0.1:1", nil)
	_, err := c.Status(context.Background())
	if !kmapi.IsTransport(err) {
		t.Fatalf("expected transport, got %v", err)
	}
}

func TestSubscribeEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/events" {
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("topics"); got != "task,balance" {
			http.Error(w, "unknown topic", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", `{"protocolVersion":"v1","instanceId":"i1","topics":["task"],"revisions":{"task":3},"state":{"task":[]}}`)
		fl.Flush()
		_, _ = w.Write([]byte(": heartbeat\n\n"))
		fl.Flush()
		fmt.Fprintf(w, "event: change\ndata: %s\n\n", `{"protocolVersion":"v1","instanceId":"i1","seq":4,"topic":"task","entityId":"abc","revision":4,"data":{}}`)
		fl.Flush()
		fmt.Fprintf(w, "event: resync\ndata: %s\n\n", `{}`)
		fl.Flush()
	}))
	defer srv.Close()
	c := kmapi.New(srv.URL, srv.Client())
	var snaps int
	var changes []kmapi.EventEnvelope
	err := c.SubscribeEvents(context.Background(), []string{"task", "balance"}, func(s kmapi.SnapshotEvent) {
		snaps++
		if s.InstanceID != "i1" {
			t.Errorf("snapshot instance = %+v", s)
		}
	}, func(e kmapi.EventEnvelope) bool {
		changes = append(changes, e)
		return true
	}, func() bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	if snaps != 1 || len(changes) != 1 {
		t.Fatalf("snaps=%d changes=%d", snaps, len(changes))
	}
	if changes[0].Seq != 4 {
		t.Fatalf("seq = %+v", changes[0])
	}
}

func TestTaskEventsRetained(t *testing.T) {
	seed := strings.Repeat("b", 64)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/tasks/"+seed+"/events" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		raw, _ := json.Marshal(map[string]any{"seedHashHex": seed, "rootID": "r_1", "state": "done", "blocksDone": 1, "blocksTotal": 1, "createdAt": "t", "updatedAt": "t"})
		fmt.Fprintf(w, "data: %s\n\n", raw)
	}))
	defer srv.Close()
	c := kmapi.New(srv.URL, srv.Client())
	var got int
	if err := c.TaskEvents(context.Background(), seed, func(kmapi.Task) bool { got++; return false }); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("got=%d", got)
	}
}
