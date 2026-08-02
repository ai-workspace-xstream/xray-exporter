package xray

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xray-exporter/internal/model"
)

type Client struct {
	url        string
	token      string
	httpClient *http.Client
}

func NewClient(url, token string) *Client {
	return &Client{
		url:        strings.TrimSpace(url),
		token:      strings.TrimSpace(token),
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

func (c *Client) FetchCounters(ctx context.Context) ([]model.RawCounter, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, fmt.Errorf("build xray stats request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch xray stats: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch xray stats: unexpected status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read xray stats payload: %w", err)
	}

	var payload struct {
		Samples []struct {
			UUID               string `json:"uuid"`
			InboundTag         string `json:"inbound_tag"`
			UplinkBytesTotal   int64  `json:"uplink_bytes_total"`
			DownlinkBytesTotal int64  `json:"downlink_bytes_total"`
		} `json:"samples"`
		Stats []struct {
			Name  string      `json:"name"`
			Value interface{} `json:"value"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		counters := parsePrometheusCounters(body)
		if len(counters) > 0 {
			return counters, nil
		}
		return nil, fmt.Errorf("decode xray stats payload: %w", err)
	}

	if len(payload.Samples) > 0 {
		counters := make([]model.RawCounter, 0, len(payload.Samples)*2)
		for _, sample := range payload.Samples {
			counters = append(counters,
				model.RawCounter{
					UUID:       strings.TrimSpace(sample.UUID),
					InboundTag: strings.TrimSpace(sample.InboundTag),
					Direction:  "uplink",
					Value:      sample.UplinkBytesTotal,
				},
				model.RawCounter{
					UUID:       strings.TrimSpace(sample.UUID),
					InboundTag: strings.TrimSpace(sample.InboundTag),
					Direction:  "downlink",
					Value:      sample.DownlinkBytesTotal,
				},
			)
		}
		return counters, nil
	}

	counters := make([]model.RawCounter, 0, len(payload.Stats))
	for _, stat := range payload.Stats {
		counter, ok := parseRawStat(stat.Name, stat.Value)
		if !ok {
			continue
		}
		counters = append(counters, counter)
	}
	if len(counters) > 0 {
		return counters, nil
	}

	var expvarPayload map[string]json.RawMessage
	if err := json.Unmarshal(body, &expvarPayload); err == nil {
		return parseExpvarCounters(expvarPayload), nil
	}
	return counters, nil
}

// parsePrometheusCounters accepts the legacy compassvpn exporter format used
// by the existing Grafana dashboards. This lets the control-plane exporter
// consume the already-deployed observation surface without changing its
// metrics or making Grafana a billing dependency.
func parsePrometheusCounters(body []byte) []model.RawCounter {
	const (
		uplinkMetric   = "xray_traffic_uplink_bytes_total"
		downlinkMetric = "xray_traffic_downlink_bytes_total"
	)

	var counters []model.RawCounter
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		metricName := line
		labels := map[string]string{}
		if brace := strings.IndexByte(line, '{'); brace >= 0 {
			metricName = strings.TrimSpace(line[:brace])
			end := strings.LastIndexByte(line, '}')
			if end < brace {
				continue
			}
			labels = parsePrometheusLabels(line[brace+1 : end])
			line = strings.TrimSpace(line[end+1:])
		} else {
			valueStart := strings.IndexAny(line, " \t")
			if valueStart < 0 {
				continue
			}
			line = strings.TrimSpace(line[valueStart:])
		}

		direction := ""
		switch metricName {
		case uplinkMetric:
			direction = "uplink"
		case downlinkMetric:
			direction = "downlink"
		default:
			continue
		}
		if dimension := strings.TrimSpace(labels["dimension"]); dimension != "" && dimension != "user" {
			continue
		}
		identifier := strings.TrimSpace(labels["target"])
		if identifier == "" {
			identifier = strings.TrimSpace(labels["email"])
		}
		if identifier == "" {
			identifier = strings.TrimSpace(labels["uuid"])
		}
		if identifier == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[0], 64)
		if err != nil || value < 0 || value > math.MaxInt64 {
			continue
		}
		inboundTag := strings.TrimSpace(labels["inbound_tag"])
		if inboundTag == "" {
			inboundTag = strings.TrimSpace(labels["inbound"])
		}
		if inboundTag == "" {
			inboundTag = strings.TrimSpace(labels["line_code"])
		}
		counters = append(counters, model.RawCounter{
			UUID:       identifier,
			InboundTag: inboundTag,
			Direction:  direction,
			Value:      int64(value),
		})
	}
	return counters
}

func parsePrometheusLabels(raw string) map[string]string {
	labels := make(map[string]string)
	for i := 0; i < len(raw); {
		for i < len(raw) && (raw[i] == ',' || raw[i] == ' ' || raw[i] == '\t') {
			i++
		}
		start := i
		for i < len(raw) && (raw[i] == '_' || raw[i] >= 'a' && raw[i] <= 'z' || raw[i] >= 'A' && raw[i] <= 'Z' || raw[i] >= '0' && raw[i] <= '9') {
			i++
		}
		if start == i {
			break
		}
		key := raw[start:i]
		for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t') {
			i++
		}
		if i >= len(raw) || raw[i] != '=' {
			break
		}
		i++
		for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t') {
			i++
		}
		if i >= len(raw) || raw[i] != '"' {
			break
		}
		i++
		var value strings.Builder
		for i < len(raw) {
			if raw[i] == '"' {
				i++
				break
			}
			if raw[i] == '\\' && i+1 < len(raw) {
				i++
				switch raw[i] {
				case 'n':
					value.WriteByte('\n')
				default:
					value.WriteByte(raw[i])
				}
				i++
				continue
			}
			value.WriteByte(raw[i])
			i++
		}
		labels[key] = value.String()
	}
	return labels
}

func parseExpvarCounters(payload map[string]json.RawMessage) []model.RawCounter {
	var statsMap map[string]interface{}
	if rawStats, ok := payload["stats"]; ok {
		_ = json.Unmarshal(rawStats, &statsMap)
	} else {
		statsMap = make(map[string]interface{}, len(payload))
		for key, rawValue := range payload {
			var value interface{}
			if err := json.Unmarshal(rawValue, &value); err != nil {
				continue
			}
			statsMap[key] = value
		}
	}

	counters := make([]model.RawCounter, 0, len(statsMap))
	for key, value := range statsMap {
		counter, ok := parseRawStat(key, value)
		if !ok {
			continue
		}
		counters = append(counters, counter)
	}
	return counters
}

func parseRawStat(name string, value interface{}) (model.RawCounter, bool) {
	parsedValue, ok := parseCounterValue(value)
	if !ok {
		return model.RawCounter{}, false
	}

	parts := strings.Split(strings.TrimSpace(name), ">>>")
	if len(parts) < 4 {
		return model.RawCounter{}, false
	}

	if parts[0] == "user" && len(parts) == 4 && parts[2] == "traffic" {
		return model.RawCounter{
			UUID:       strings.TrimSpace(parts[1]),
			InboundTag: "",
			Direction:  strings.TrimSpace(parts[3]),
			Value:      parsedValue,
		}, true
	}

	if parts[0] == "inbound" && len(parts) == 6 && parts[2] == "user" && parts[4] == "traffic" {
		return model.RawCounter{
			UUID:       strings.TrimSpace(parts[3]),
			InboundTag: strings.TrimSpace(parts[1]),
			Direction:  strings.TrimSpace(parts[5]),
			Value:      parsedValue,
		}, true
	}

	return model.RawCounter{}, false
}

func parseCounterValue(value interface{}) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
