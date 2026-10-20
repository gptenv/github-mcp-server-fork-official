package github

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ghErrors "github.com/github/github-mcp-server/v2/pkg/errors"
	"github.com/github/github-mcp-server/v2/pkg/utils"
	"github.com/google/go-github/v92/github"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const actionsMethodWatchWorkflowRun = "watch_workflow_run"

const (
	defaultWatchTimeoutSeconds = 45
	maxWatchTimeoutSeconds     = 600
)

// WatchConfig configures how often watch_workflow_run polls a workflow run.
type WatchConfig struct {
	PollInterval time.Duration
}

type watchConfigKey struct{}

// ContextWithWatchConfig returns a context with workflow run watch configuration.
// Use this in tests to poll without waiting.
func ContextWithWatchConfig(ctx context.Context, config WatchConfig) context.Context {
	return context.WithValue(ctx, watchConfigKey{}, config)
}

func getWatchConfig(ctx context.Context) WatchConfig {
	if config, ok := ctx.Value(watchConfigKey{}).(WatchConfig); ok {
		return config
	}
	return WatchConfig{PollInterval: 10 * time.Second}
}

// MinimalFailedWorkflowJob identifies a job that did not succeed in a watched workflow run.
type MinimalFailedWorkflowJob struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Conclusion string `json:"conclusion,omitempty"`
	HTMLURL    string `json:"html_url,omitempty"`
}

// MinimalWorkflowRunWatchResult is the output type for the watch_workflow_run method.
type MinimalWorkflowRunWatchResult struct {
	WorkflowRun   MinimalWorkflowRun         `json:"workflow_run"`
	Completed     bool                       `json:"completed"`
	WaitedSeconds int                        `json:"waited_seconds"`
	FailedJobs    []MinimalFailedWorkflowJob `json:"failed_jobs,omitempty"`
	NextStep      string                     `json:"next_step,omitempty"`
}

// watchWorkflowRun polls a workflow run until it completes or the timeout elapses.
// Reaching the timeout is not an error: the result reports the current status so the caller can watch again.
func watchWorkflowRun(ctx context.Context, client *github.Client, request *mcp.CallToolRequest, owner, repo string, runID int64, timeout time.Duration) (*mcp.CallToolResult, *ActionsGetOutput, error) {
	pollInterval := getWatchConfig(ctx).PollInterval
	start := time.Now()
	deadline := start.Add(timeout)

	var progressToken any
	if request != nil && request.Params != nil {
		progressToken = request.Params.GetProgressToken()
	}

	for poll := 1; ; poll++ {
		workflowRun, resp, err := client.Actions.GetWorkflowRunByID(ctx, owner, repo, runID)
		if err != nil {
			return ghErrors.NewGitHubAPIErrorResponse(ctx, "failed to get workflow run", resp, err), nil, nil
		}
		_ = resp.Body.Close()

		completed := workflowRun.GetStatus() == "completed"
		remaining := time.Until(deadline)
		if completed || remaining <= 0 {
			return watchWorkflowRunResult(ctx, client, owner, repo, workflowRun, completed, time.Since(start))
		}

		if progressToken != nil && request.Session != nil {
			_ = request.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{
				ProgressToken: progressToken,
				Progress:      float64(poll),
				Message:       fmt.Sprintf("Workflow run %d is %s (waited %ds)", runID, workflowRun.GetStatus(), int(time.Since(start).Seconds())),
			})
		}

		timer := time.NewTimer(min(pollInterval, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil, fmt.Errorf("stopped watching workflow run %d: %w", runID, ctx.Err())
		case <-timer.C:
		}
	}
}

func watchWorkflowRunResult(ctx context.Context, client *github.Client, owner, repo string, workflowRun *github.WorkflowRun, completed bool, waited time.Duration) (*mcp.CallToolResult, *ActionsGetOutput, error) {
	result := MinimalWorkflowRunWatchResult{
		WorkflowRun:   convertToMinimalWorkflowRun(workflowRun),
		Completed:     completed,
		WaitedSeconds: int(waited.Seconds()),
	}

	switch {
	case !completed:
		result.NextStep = fmt.Sprintf("The workflow run is still %s. Call watch_workflow_run again to keep waiting.", workflowRun.GetStatus())
	case !isSuccessfulConclusion(workflowRun.GetConclusion()):
		jobs, resp, err := client.Actions.ListWorkflowJobs(ctx, owner, repo, workflowRun.GetID(), &github.ListWorkflowJobsOptions{
			Filter: "latest",
		})
		if err != nil {
			return ghErrors.NewGitHubAPIErrorResponse(ctx, "failed to list workflow jobs", resp, err), nil, nil
		}
		_ = resp.Body.Close()

		for _, job := range jobs.Jobs {
			if !isSuccessfulConclusion(job.GetConclusion()) {
				result.FailedJobs = append(result.FailedJobs, MinimalFailedWorkflowJob{
					ID:         job.GetID(),
					Name:       job.GetName(),
					Conclusion: job.GetConclusion(),
					HTMLURL:    job.GetHTMLURL(),
				})
			}
		}
		result.NextStep = fmt.Sprintf("Use get_job_logs with run_id %d and failed_only true to read the logs of the failed jobs.", workflowRun.GetID())
	}

	r, err := json.Marshal(result)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal workflow run watch result: %w", err)
	}
	return utils.NewToolResultText(string(r)), &ActionsGetOutput{Method: actionsMethodWatchWorkflowRun, Watch: &result}, nil
}

// isSuccessfulConclusion reports whether a workflow run or job conclusion needs no attention.
func isSuccessfulConclusion(conclusion string) bool {
	switch conclusion {
	case "success", "neutral", "skipped":
		return true
	default:
		return false
	}
}
