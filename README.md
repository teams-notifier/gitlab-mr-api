# gitlab-mr-api

Teams Notifier's gitlab-mr-api component.

Environment variables or `.env`:

* `PORT`: port to listen to (def: `3980`)
* `ACTIVITY_API`: `activity-api` base URL (ex: `https://activity-api:3981/`)
* `DATABASE_URL`: Database DSN in the form: `postgresql://{USER}:{PASSWORD}@{HOST}/{DATABASE}`
* `VALID_X_GITLAB_TOKEN`: comma separated list of Gitlab's Secret token (sent as `X-Gitlab-Token` header), uuidv4 generated seems a good choice.

# Usage

You'll need:
- one or more conversation tokens
- one of the `VALID_X_GITLAB_TOKEN` you generated

On your Gitlab repo, go to Settings then Webhooks and click "Add new webhook" and fill up the form:

* **URL**: https://hostname-of-this-api.example.org/api/v1/gitlab-webhook
* **Add custom header**:
  * name: `X-Conversation-Token`
  * value: comma separated list of conversation tokens you want the MR messages sent to
* **Secret token**: one of the `VALID_X_GITLAB_TOKEN` you generated
* **Trigger**, select:
  * Merge request events
  * Pipeline events
  * Emoji (soon)

You can then save or test.
Note that for the test to work you need an existing MR in the project.


In case of failure, now or afterward, you can Edit the webhook and you'll see all the previous calls, error code and messages, including sent payload and a way to resend the request.
