package notifs

import (
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/disgo/webhook"
	"github.com/disgoorg/snowflake/v2"

	"github.com/ceramicnetwork/go-cas/common"
	"github.com/ceramicnetwork/go-cas/models"
)

type DiscordColor int

const (
	DiscordColor_None    = iota
	DiscordColor_Info    = 3447003
	DiscordColor_Ok      = 3581519
	DiscordColor_Warning = 16776960
	DiscordColor_Alert   = 16711712
)

const DiscordPacing = 2 * time.Second

type DiscordHandler struct {
	alertWebhook   webhook.Client
	warningWebhook webhook.Client
	testWebhook    webhook.Client
	logger         models.Logger
}

func NewDiscordHandler(logger models.Logger) (models.Notifier, error) {
	if a, err := parseDiscordWebhookUrl("DISCORD_ALERT_WEBHOOK"); err != nil {
		return nil, err
	} else if w, err := parseDiscordWebhookUrl("DISCORD_WARNING_WEBHOOK"); err != nil {
		return nil, err
	} else if t, err := parseDiscordWebhookUrl("DISCORD_TEST_WEBHOOK"); err != nil {
		return nil, err
	} else {
		return &DiscordHandler{a, w, t, logger}, nil
	}
}

func parseDiscordWebhookUrl(urlEnv string) (webhook.Client, error) {
	webhookUrl := os.Getenv(urlEnv)
	if len(webhookUrl) > 0 {
		if parsedUrl, err := url.Parse(webhookUrl); err != nil {
			return nil, err
		} else {
			urlParts := strings.Split(parsedUrl.Path, "/")
			if id, err := snowflake.Parse(urlParts[len(urlParts)-2]); err != nil {
				return nil, err
			} else {
				return webhook.New(id, urlParts[len(urlParts)-1]), nil
			}
		}
	}
	return nil, nil
}

func (d DiscordHandler) SendAlert(title, desc string) error {
	if d.alertWebhook != nil {
		return d.sendNotif(d.alertWebhook, title, desc, DiscordColor_Alert)
	}
	// Always duplicate notifications to the test channel, if configured.
	if d.testWebhook != nil {
		return d.sendNotif(d.testWebhook, title, desc, DiscordColor_Alert)
	}
	return nil
}

// TODO: Need to make the output more readable for specific errors
func (d DiscordHandler) sendNotif(wh webhook.Client, title, desc string, color DiscordColor) error {
	messageEmbed := discord.Embed{
		Title:       title,
		Description: desc,
		Type:        discord.EmbedTypeRich,
		Color:       int(color),
	}
	_, err := wh.CreateMessage(discord.NewWebhookMessageCreateBuilder().
		SetEmbeds(messageEmbed).
		SetUsername(common.ServiceName).
		Build(),
		rest.WithDelay(DiscordPacing),
	)
	if err != nil {
		d.logger.Errorf("sendNotif: error sending discord notification: %v, %s, %s", err, title, desc)
		return err
	}
	return nil
}
