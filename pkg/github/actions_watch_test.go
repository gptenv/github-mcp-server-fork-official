package github

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/github/github-mcp-server/v2/pkg/translations"
	"github.com/google/go-github/v92/github"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// workflowRunSequence serves the given status/conclusion pairs in order, repeating the last one.
// It returns the handler and a function that reports how many times the run was fetched.
func workflowRunSequence(states ...[2]string) (http.HandlerFunc, func() int) {
	var calls atomic.Int32
	handler := func(w http.ResponseWriter, _ *http.Request) {
		i := int(calls.Add(1)) - 1
		state := states[min(i, len(states)-1)]
		run := actionsTestWorkflowRun()
		run.Status = new(state[0])
		run.Conclusion = new(state[1])
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(run)
	}
	return handler, func() int { return int(calls.Load()) }
}

func watchedRun(status, conclusion string) MinimalWorkflowRun {
	run := actionsTestWorkflowRun()
	run.Status = new(status)
	run.Conclusion = new(conclusion)
	return convertToMinimalWorkflowRun(run)
}

func Test_ActionsGet_WatchWorkflowRun(t *testing.T) {
	toolDef := ActionsGet(translations.NullTranslationHelper)

	failedRunJobs := &github.Jobs{
		TotalCount: new(3),
		Jobs: []*github.WorkflowJob{
			{ID: new(int64(1)), Name: new("lint"), Status: new("completed"), Conclusion: new("success")},
			{ID: new(int64(2)), Name: new("test"), Status: new("completed"), Conclusion: new("failure"), HTMLURL: new("https://github.com/octo-org/octo-repo/actions/runs/30433642/job/2")},
			{ID: new(int64(3)), Name: new("deploy"), Status: new("completed"), Conclusion: new("cancelled")},
		},
	}

	tests := []struct {
		name          string
		states        [][2]string
		jobs          *github.Jobs
		timeout       int
		expectedCalls int
		expected      MinimalWorkflowRunWatchResult
	}{
		{
			name:          "returns at once when the run already completed",
			states:        [][2]string{{"completed", "success"}},
			expectedCalls: 1,
			expected: MinimalWorkflowRunWatchResult{
				WorkflowRun: watchedRun("completed", "success"),
				Completed:   true,
			},
		},
		{
			name:          "waits until the run completes",
			states:        [][2]string{{"queued", ""}, {"in_progress", ""}, {"completed", "success"}},
			expectedCalls: 3,
			expected: MinimalWorkflowRunWatchResult{
				WorkflowRun: watchedRun("completed", "success"),
				Completed:   true,
			},
		},
		{
			name:          "lists the jobs that did not succeed when the run fails",
			states:        [][2]string{{"in_progress", ""}, {"completed", "failure"}},
			jobs:          failedRunJobs,
			expectedCalls: 2,
			expected: MinimalWorkflowRunWatchResult{
				WorkflowRun: watchedRun("completed", "failure"),
				Completed:   true,
				FailedJobs: []MinimalFailedWorkflowJob{
					{ID: 2, Name: "test", Conclusion: "failure", HTMLURL: "https://github.com/octo-org/octo-repo/actions/runs/30433642/job/2"},
					{ID: 3, Name: "deploy", Conclusion: "cancelled"},
				},
				NextStep: "Use get_job_logs with run_id 30433642 and failed_only true to read the logs of the failed jobs.",
			},
		},
		{
			name:    "returns the current status when the timeout elapses",
			states:  [][2]string{{"in_progress", ""}},
			timeout: 1,
			expected: MinimalWorkflowRunWatchResult{
				WorkflowRun:   watchedRun("in_progress", ""),
				Completed:     false,
				WaitedSeconds: 1,
				NextStep:      "The workflow run is still in_progress. Call watch_workflow_run again to keep waiting.",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runHandler, calls := workflowRunSequence(tc.states...)
			handlers := map[string]http.HandlerFunc{
				GetReposActionsRunsByOwnerByRepoByRunID: runHandler,
			}
			if tc.jobs != nil {
				handlers[GetReposActionsRunsJobsByOwnerByRepoByRunID] = mockResponse(t, http.StatusOK, tc.jobs)
			}
			deps := BaseDeps{Client: mustNewGHClient(t, MockHTTPClientWithHandlers(handlers))}
			handler := toolDef.Handler(deps)

			args := map[string]any{
				"method":      "watch_workflow_run",
				"owner":       "owner",
				"repo":        "repo",
				"resource_id": "30433642",
			}
			if tc.timeout > 0 {
				args["timeout_seconds"] = tc.timeout
			}
			request := createMCPRequest(args)
			ctx := ContextWithWatchConfig(ContextWithDeps(context.Background(), deps), WatchConfig{PollInterval: 10 * time.Millisecond})
			result, err := handler(ctx, &request)
			require.NoError(t, err)
			require.False(t, result.IsError, getTextResult(t, result).Text)

			var response MinimalWorkflowRunWatchResult
			require.NoError(t, json.Unmarshal([]byte(getTextResult(t, result).Text), &response))
			assert.Equal(t, tc.expected, response)
			if tc.expectedCalls > 0 {
				assert.Equal(t, tc.expectedCalls, calls())
			}
		})
	}
}

func Test_ActionsGet_WatchWorkflowRun_Errors(t *testing.T) {
	toolDef := ActionsGet(translations.NullTranslationHelper)

	tests := []struct {
		name           string
		handler        http.HandlerFunc
		args           map[string]any
		expectedErrMsg string
	}{
		{
			name:           "timeout above the maximum",
			args:           map[string]any{"timeout_seconds": 601},
			expectedErrMsg: "timeout_seconds must be between 1 and 600",
		},
		{
			name:           "negative timeout",
			args:           map[string]any{"timeout_seconds": -5},
			expectedErrMsg: "timeout_seconds must be between 1 and 600",
		},
		{
			name:           "run ID is not a number",
			args:           map[string]any{"resource_id": "ci.yml"},
			expectedErrMsg: "invalid resource_id, must be an integer for method watch_workflow_run",
		},
		{
			name:           "run not found",
			handler:        mockResponse(t, http.StatusNotFound, map[string]string{"message": "Not Found"}),
			expectedErrMsg: "failed to get workflow run",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handlers := map[string]http.HandlerFunc{}
			if tc.handler != nil {
				handlers[GetReposActionsRunsByOwnerByRepoByRunID] = tc.handler
			}
			deps := BaseDeps{Client: mustNewGHClient(t, MockHTTPClientWithHandlers(handlers))}
			handler := toolDef.Handler(deps)

			args := map[string]any{
				"method":      "watch_workflow_run",
				"owner":       "owner",
				"repo":        "repo",
				"resource_id": "30433642",
			}
			maps.Copy(args, tc.args)
			request := createMCPRequest(args)
			result, err := handler(ContextWithDeps(context.Background(), deps), &request)
			require.NoError(t, err)
			assert.Contains(t, getErrorResult(t, result).Text, tc.expectedErrMsg)
		})
	}
}

func Test_ActionsGet_WatchWorkflowRun_StopsWhenCancelled(t *testing.T) {
	toolDef := ActionsGet(translations.NullTranslationHelper)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runHandler, calls := workflowRunSequence([2]string{"in_progress", ""})
	deps := BaseDeps{Client: mustNewGHClient(t, MockHTTPClientWithHandlers(map[string]http.HandlerFunc{
		GetReposActionsRunsByOwnerByRepoByRunID: func(w http.ResponseWriter, r *http.Request) {
			runHandler(w, r)
			cancel()
		},
	}))}
	handler := toolDef.Handler(deps)

	request := createMCPRequest(map[string]any{
		"method":          "watch_workflow_run",
		"owner":           "owner",
		"repo":            "repo",
		"resource_id":     "30433642",
		"timeout_seconds": 600,
	})
	ctx = ContextWithWatchConfig(ContextWithDeps(ctx, deps), WatchConfig{PollInterval: time.Hour})
	_, err := handler(ctx, &request)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls())
}

func Test_ActionsGet_WatchWorkflowRun_SendsProgress(t *testing.T) {
	toolDef := ActionsGet(translations.NullTranslationHelper)
	runHandler, _ := workflowRunSequence([2]string{"queued", ""}, [2]string{"in_progress", ""}, [2]string{"completed", "success"})
	deps := BaseDeps{Client: mustNewGHClient(t, MockHTTPClientWithHandlers(map[string]http.HandlerFunc{
		GetReposActionsRunsByOwnerByRepoByRunID: runHandler,
	}))}
	handler := toolDef.Handler(deps)

	var mu sync.Mutex
	var messages []string
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, &mcp.ClientOptions{
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			mu.Lock()
			defer mu.Unlock()
			messages = append(messages, req.Params.Message)
		},
	})
	server := mcp.NewServer(&mcp.Implementation{Name: "test-server"}, nil)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	serverSession, err := server.Connect(context.Background(), serverTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = serverSession.Close() })
	clientSession, err := client.Connect(context.Background(), clientTransport, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientSession.Close() })

	argsJSON, err := json.Marshal(map[string]any{
		"method":      "watch_workflow_run",
		"owner":       "owner",
		"repo":        "repo",
		"resource_id": "30433642",
	})
	require.NoError(t, err)
	request := mcp.CallToolRequest{
		Session: serverSession,
		Params: &mcp.CallToolParamsRaw{
			Meta:      mcp.Meta{"progressToken": "watch-1"},
			Arguments: argsJSON,
		},
	}
	ctx := ContextWithWatchConfig(ContextWithDeps(context.Background(), deps), WatchConfig{PollInterval: 10 * time.Millisecond})
	result, err := handler(ctx, &request)
	require.NoError(t, err)
	require.False(t, result.IsError)

	expected := []string{
		"Workflow run 30433642 is queued (waited 0s)",
		"Workflow run 30433642 is in_progress (waited 0s)",
	}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		mu.Lock()
		defer mu.Unlock()
		assert.Equal(c, expected, messages)
	}, time.Second, 10*time.Millisecond)
}

func TestTypedActionsWatchWorkflowRunWire(t *testing.T) {
	for _, protocol := range typedActionsProtocols {
		for _, state := range [][2]string{{"completed", "success"}, {"completed", "failure"}, {"in_progress", ""}} {
			t.Run(protocol+"/"+state[0]+"/"+state[1], func(t *testing.T) {
				runHandler, _ := workflowRunSequence(state)
				deps := BaseDeps{Client: mustNewGHClient(t, MockHTTPClientWithHandlers(map[string]http.HandlerFunc{
					GetReposActionsRunsByOwnerByRepoByRunID: runHandler,
					GetReposActionsRunsJobsByOwnerByRepoByRunID: mockResponse(t, http.StatusOK, &github.Jobs{
						TotalCount: new(1),
						Jobs:       []*github.WorkflowJob{{ID: new(int64(2)), Name: new("test"), Status: new("completed"), Conclusion: new("failure")}},
					}),
				}))}
				session, schemas := actionsTypedSession(t, deps, protocol)
				result, err := session.CallTool(context.Background(), &mcp.CallToolParams{
					Name:      "actions_get",
					Arguments: map[string]any{"method": "watch_workflow_run", "owner": "owner", "repo": "repo", "resource_id": "30433642", "timeout_seconds": "1"},
				})
				require.NoError(t, err)
				expected := MinimalWorkflowRunWatchResult{WorkflowRun: watchedRun(state[0], state[1]), Completed: state[0] == "completed"}
				if !expected.Completed {
					expected.WaitedSeconds = 1
					expected.NextStep = "The workflow run is still in_progress. Call watch_workflow_run again to keep waiting."
				} else if state[1] == "failure" {
					expected.FailedJobs = []MinimalFailedWorkflowJob{{ID: 2, Name: "test", Conclusion: "failure"}}
					expected.NextStep = "Use get_job_logs with run_id 30433642 and failed_only true to read the logs of the failed jobs."
				}
				projected := mustMarshalJSON(t, map[string]any{"method": "watch_workflow_run", "watch": expected})
				assertActionsWireResult(t, result, schemas["actions_get"], mustMarshalJSON(t, expected), false, projected)
			})
		}
	}
}
