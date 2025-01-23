package slack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/skip-mev/connect-mmu/lib/aws"
)

type slackMessage struct {
	Text string `json:"text"`
}

func SendNotification(message string, webhookURLSecretName string) error {
	webhookURL, err := aws.GetSecret(context.Background(), webhookURLSecretName)
	if err != nil {
		fmt.Printf("Error fetching Slack Webhook URL from Secrets Manager: %v", err)
		return err
	}

	slackMessage := slackMessage{Text: message}
	slackBody, _ := json.Marshal(slackMessage)
	req, err := http.NewRequest(http.MethodPost, webhookURL, bytes.NewBuffer(slackBody))
	if err != nil {
		fmt.Printf("Error creating Slack request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending Slack request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Error response from Slack: %v", resp.StatusCode)
	} else {
		fmt.Printf("Successfully sent Slack notification: %v", resp.StatusCode)
	}

	return nil
}
