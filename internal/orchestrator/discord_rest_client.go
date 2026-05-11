package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type DiscordRESTClient struct {
	token      string
	baseURL    string
	httpClient *http.Client
}

func NewDiscordRESTClient(token string) DiscordRESTClient {
	return DiscordRESTClient{
		token:      token,
		baseURL:    "https://discord.com/api/v10",
		httpClient: http.DefaultClient,
	}
}

func (c DiscordRESTClient) Me(ctx context.Context) (DiscordUser, error) {
	var user DiscordUser
	if err := c.do(ctx, http.MethodGet, "/users/@me", nil, &user); err != nil {
		return DiscordUser{}, err
	}
	return user, nil
}

func (c DiscordRESTClient) ListMessages(ctx context.Context, channelID string, afterID string, limit int) ([]DiscordMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	path := "/channels/" + url.PathEscape(channelID) + "/messages?limit=" + url.QueryEscape(fmt.Sprintf("%d", limit))
	if strings.TrimSpace(afterID) != "" {
		path += "&after=" + url.QueryEscape(afterID)
	}
	var messages []DiscordMessage
	if err := c.do(ctx, http.MethodGet, path, nil, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}

func (c DiscordRESTClient) SendMessage(ctx context.Context, channelID string, content string) error {
	body := map[string]string{"content": truncateDiscordMessage(content)}
	return c.do(ctx, http.MethodPost, "/channels/"+url.PathEscape(channelID)+"/messages", body, nil)
}

func (c DiscordRESTClient) do(ctx context.Context, method string, path string, body any, out any) error {
	if strings.TrimSpace(c.token) == "" {
		return errors.New("discord bot token is required")
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.baseURL, "/")+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bot "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := c.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("discord api %s %s: status %d: %s", method, path, res.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			return err
		}
	}
	return nil
}
