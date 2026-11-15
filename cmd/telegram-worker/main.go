// Command telegram-worker sends Telegram Bot API notifications on torrent events.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

type settings struct {
	Enabled          bool   `json:"enabled"`
	BotToken         string `json:"botToken"`
	ChatID           string `json:"chatId"`
	NotifyAdded      bool   `json:"notifyAdded"`
	NotifyFinished   bool   `json:"notifyFinished"`
	NotifyRemoved    bool   `json:"notifyRemoved"`
	IntervalSec      int    `json:"intervalSec"`
	TemplateAdded    string `json:"templateAdded"`
	TemplateFinished string `json:"templateFinished"`
	TemplateRemoved  string `json:"templateRemoved"`
}

type snapRow struct {
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	SizeBytes int64  `json:"sizeBytes"`
	Complete  bool   `json:"complete"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	go loopScan()

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		switch req.Method {
		case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
			reply(req.ID, map[string]any{"ok": true})
		case "fleet.shutdown":
			reply(req.ID, map[string]any{"ok": true})
			return
		case "ext.telegram.getSettings":
			st, err := loadSettings()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, publicSettings(st))
		case "ext.telegram.saveSettings":
			var p settings
			if err := json.Unmarshal(req.Params, &p); err != nil {
				replyErr(req.ID, "invalid settings")
				continue
			}
			cur, _ := loadSettings()
			var extra struct {
				ClearToken bool `json:"clearToken"`
			}
			_ = json.Unmarshal(req.Params, &extra)
			if strings.TrimSpace(p.BotToken) == "" && !extra.ClearToken {
				p.BotToken = cur.BotToken
			}
			if extra.ClearToken {
				p.BotToken = ""
			}
			if err := validateSettings(p); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			if err := saveSettings(&p); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, publicSettings(p))
		case "ext.telegram.test":
			st, err := loadSettings()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			if err := sendMessage(st, "Fleet Telegram test message"); err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, map[string]any{"ok": true})
		case "ext.telegram.scan":
			res, err := scanOnce()
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, res)
		default:
			replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
		}
	}
}

func loopScan() {
	time.Sleep(3 * time.Second)
	for {
		st, err := loadSettings()
		interval := 15
		if err == nil && st.IntervalSec > 0 {
			interval = st.IntervalSec
		}
		if err == nil && st.Enabled {
			_, _ = scanOnce()
		}
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func defaultSettings() settings {
	return settings{
		Enabled:          false,
		NotifyAdded:      true,
		NotifyFinished:   true,
		NotifyRemoved:    true,
		IntervalSec:      15,
		TemplateAdded:    "Added: {name}",
		TemplateFinished: "Finished: {name}",
		TemplateRemoved:  "Removed: {name}",
	}
}

func publicSettings(st settings) map[string]any {
	return map[string]any{
		"enabled":          st.Enabled,
		"chatId":           st.ChatID,
		"tokenConfigured":  strings.TrimSpace(st.BotToken) != "",
		"notifyAdded":      st.NotifyAdded,
		"notifyFinished":   st.NotifyFinished,
		"notifyRemoved":    st.NotifyRemoved,
		"intervalSec":      st.IntervalSec,
		"templateAdded":    st.TemplateAdded,
		"templateFinished": st.TemplateFinished,
		"templateRemoved":  st.TemplateRemoved,
	}
}

func validateSettings(st settings) error {
	if st.IntervalSec < 5 || st.IntervalSec > 3600 {
		return fmt.Errorf("intervalSec must be 5..3600")
	}
	if strings.TrimSpace(st.ChatID) == "" {
		return fmt.Errorf("chatId required")
	}
	if st.Enabled && strings.TrimSpace(st.BotToken) == "" {
		return fmt.Errorf("botToken required when enabled")
	}
	return nil
}

func loadSettings() (settings, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "settings"})
	if err != nil {
		return settings{}, err
	}
	if string(raw) == "null" || len(raw) == 0 {
		return defaultSettings(), nil
	}
	var st settings
	if err := json.Unmarshal(raw, &st); err != nil {
		return defaultSettings(), nil
	}
	if st.IntervalSec == 0 {
		st.IntervalSec = 15
	}
	if st.TemplateAdded == "" {
		st.TemplateAdded = defaultSettings().TemplateAdded
	}
	if st.TemplateFinished == "" {
		st.TemplateFinished = defaultSettings().TemplateFinished
	}
	if st.TemplateRemoved == "" {
		st.TemplateRemoved = defaultSettings().TemplateRemoved
	}
	return st, nil
}

func saveSettings(st *settings) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "settings", "value": st})
	return err
}

func loadSnap() (map[string]snapRow, error) {
	raw, err := hostCall("fleet.storage.get", map[string]any{"key": "snapshot"})
	if err != nil {
		return nil, err
	}
	out := map[string]snapRow{}
	if string(raw) == "null" || len(raw) == 0 {
		return out, nil
	}
	_ = json.Unmarshal(raw, &out)
	return out, nil
}

func saveSnap(snap map[string]snapRow) error {
	_, err := hostCall("fleet.storage.put", map[string]any{"key": "snapshot", "value": snap})
	return err
}

func scanOnce() (map[string]any, error) {
	st, err := loadSettings()
	if err != nil {
		return nil, err
	}
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return nil, err
	}
	var payload struct {
		Torrents []snapRow `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	prev, err := loadSnap()
	if err != nil {
		return nil, err
	}
	cur := map[string]snapRow{}
	sent := 0
	first := len(prev) == 0
	for _, t := range payload.Torrents {
		hash := strings.ToUpper(strings.TrimSpace(t.Hash))
		if hash == "" {
			continue
		}
		t.Hash = hash
		cur[hash] = t
		old, ok := prev[hash]
		if first {
			continue
		}
		if !ok && st.NotifyAdded {
			if err := sendMessage(st, render(st.TemplateAdded, t)); err == nil {
				sent++
			}
		} else if ok && !old.Complete && t.Complete && st.NotifyFinished {
			if err := sendMessage(st, render(st.TemplateFinished, t)); err == nil {
				sent++
			}
		}
	}
	if !first && st.NotifyRemoved {
		for hash, old := range prev {
			if _, ok := cur[hash]; !ok {
				if err := sendMessage(st, render(st.TemplateRemoved, old)); err == nil {
					sent++
				}
			}
		}
	}
	if err := saveSnap(cur); err != nil {
		return nil, err
	}
	return map[string]any{"sent": sent, "tracked": len(cur), "prime": first}, nil
}

func render(tmpl string, t snapRow) string {
	s := tmpl
	s = strings.ReplaceAll(s, "{name}", t.Name)
	s = strings.ReplaceAll(s, "{hash}", t.Hash)
	s = strings.ReplaceAll(s, "{size}", fmt.Sprintf("%d", t.SizeBytes))
	return s
}

func sendMessage(st settings, text string) error {
	token := strings.TrimSpace(st.BotToken)
	chat := strings.TrimSpace(st.ChatID)
	if token == "" || chat == "" {
		return fmt.Errorf("botToken and chatId required")
	}
	form := url.Values{}
	form.Set("chat_id", chat)
	form.Set("text", text)
	apiURL := "https://api.telegram.org/bot" + token + "/sendMessage"
	raw, err := hostCall("fleet.http.fetch", map[string]any{
		"url":         apiURL,
		"method":      "POST",
		"contentType": "application/x-www-form-urlencoded",
		"body":        form.Encode(),
	})
	if err != nil {
		return err
	}
	var out struct {
		Status int    `json:"status"`
		Body   string `json:"body"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return err
	}
	if out.Status >= 400 {
		return fmt.Errorf("telegram HTTP %d: %s", out.Status, truncate(out.Body, 200))
	}
	var tg struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}
	if err := json.Unmarshal([]byte(out.Body), &tg); err == nil && !tg.OK {
		return fmt.Errorf("telegram: %s", tg.Description)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func hostCall(method string, params any) (json.RawMessage, error) {
	ch := make(chan hostReply, 1)
	mu.Lock()
	id := nextHost
	nextHost++
	pending[id] = ch
	mu.Unlock()

	b, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      id,
	})
	if err != nil {
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, err
	}
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()

	select {
	case reply := <-ch:
		if reply.err != "" {
			return nil, fmt.Errorf("%s", reply.err)
		}
		if len(reply.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return reply.result, nil
	case <-time.After(30 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "result": result, "id": id})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func replyErr(id any, msg string) {
	b, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"error":   map[string]any{"code": -32000, "message": msg},
		"id":      id,
	})
	outMu.Lock()
	fmt.Fprintf(os.Stdout, "%s\n", b)
	outMu.Unlock()
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}
